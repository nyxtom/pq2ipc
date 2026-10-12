// Parquet -> Arrow IPC benchmark: two parquet::arrow baselines vs the
// page-level transcoder, over a simulated GCS link. One mode per process so
// peak memory numbers are not polluted by earlier runs.
//
//   bench --mode=table|stream|transcode --file=images.parquet [--net=1]
//   bench --mode=verify --a=out_table --b=out_transcode
#include <arrow/compute/api.h>
#include <arrow/compute/initialize.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#include <cstdio>
#include <filesystem>
#include <string>

#include "sim.h"
#include "transcode.h"

static std::string Arg(int argc, char** argv, const char* name, const char* def) {
  std::string prefix = std::string("--") + name + "=";
  for (int i = 1; i < argc; ++i)
    if (std::strncmp(argv[i], prefix.c_str(), prefix.size()) == 0) return argv[i] + prefix.size();
  return def;
}

// The "tiny bit of filtering": drop every 10th id.
static arrow::Result<std::vector<uint8_t>> KeepRows(const arrow::Table& t) {
  auto col = t.GetColumnByName("id");
  std::vector<uint8_t> keep;
  keep.reserve(t.num_rows());
  for (auto& chunk : col->chunks()) {
    auto ids = std::static_pointer_cast<arrow::Int64Array>(chunk);
    for (int64_t i = 0; i < ids->length(); ++i) keep.push_back(ids->Value(i) % 10 != 0);
  }
  return keep;
}

static arrow::Result<std::shared_ptr<arrow::Array>> KeepMask(const arrow::Table& t) {
  ARROW_ASSIGN_OR_RAISE(auto keep, KeepRows(t));
  arrow::BooleanBuilder b;
  ARROW_RETURN_NOT_OK(b.Reserve(keep.size()));
  for (auto k : keep) b.UnsafeAppend(k != 0);
  return b.Finish();
}

struct Shared {
  std::string mode, out_dir;
  int64_t part_bytes = 8 << 20;
  Uploader* uploader = nullptr;
  NetModel* down = nullptr;
  std::atomic<int64_t> rows_out{0};
};

// Everything opened once per input file. Opening costs a footer read, a page
// index read and a schema build: the per-file startup we want to overlap.
struct Ctx {
  Shared* s;
  std::string name;
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  std::shared_ptr<parquet::FileMetaData> md;
  int nrg = 0;
  TranscodeFile tf;
};

// Each row group becomes one output object. With --out-dir the parts are
// written to a local file in order (verification); otherwise they go through
// the simulated upload.
static std::unique_ptr<PartOutputStream> MakeSink(Ctx& c, int rg) {
  if (!c.s->out_dir.empty()) {
    auto path = c.s->out_dir + "/" + c.name + "_rg_" + std::to_string(rg) + ".arrows";
    auto f = *arrow::io::FileOutputStream::Open(path);
    return std::make_unique<PartOutputStream>(c.s->part_bytes, [f](auto part) -> arrow::Status {
      for (auto& b : part) ARROW_RETURN_NOT_OK(f->Write(b->data(), b->size()));
      return arrow::Status::OK();
    });
  }
  return std::make_unique<PartOutputStream>(c.s->part_bytes, [&c](auto part) {
    c.s->uploader->Submit(std::move(part));
    return arrow::Status::OK();
  });
}

static arrow::Result<std::unique_ptr<parquet::arrow::FileReader>> OpenReader(Ctx& c, int64_t batch_rows) {
  parquet::ArrowReaderProperties props;
  props.set_pre_buffer(true);
  props.set_use_threads(true);
  props.set_batch_size(batch_rows);
  parquet::arrow::FileReaderBuilder b;
  ARROW_RETURN_NOT_OK(b.Open(c.file, parquet::default_reader_properties(), c.md));
  std::unique_ptr<parquet::arrow::FileReader> r;
  ARROW_RETURN_NOT_OK(b.properties(props)->Build(&r));
  return r;
}

// Baseline A: ReadRowGroup -> Filter -> WriteTable. The usual approach.
static arrow::Status RunTable(Ctx& c, int rg) {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(c, 64 * 1024));
  std::shared_ptr<arrow::Table> t;
  ARROW_RETURN_NOT_OK(reader->ReadRowGroup(rg, &t));
  ARROW_ASSIGN_OR_RAISE(auto mask, KeepMask(*t));
  ARROW_ASSIGN_OR_RAISE(arrow::Datum f, arrow::compute::Filter(t, mask));
  t = f.table();
  auto sink = MakeSink(c, rg);
  std::shared_ptr<arrow::io::OutputStream> sp(sink.get(), [](void*) {});
  ARROW_ASSIGN_OR_RAISE(auto w, arrow::ipc::MakeStreamWriter(sp, t->schema()));
  ARROW_RETURN_NOT_OK(w->WriteTable(*t));
  ARROW_RETURN_NOT_OK(w->Close());
  c.s->rows_out += t->num_rows();
  return sink->Close();
}

// Baseline B: tuned Arrow. Streaming record batch reader with small batches.
static arrow::Status RunStream(Ctx& c, int rg, int64_t batch_rows) {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(c, batch_rows));
  ARROW_ASSIGN_OR_RAISE(auto rbr, reader->GetRecordBatchReader({rg}));
  auto sink = MakeSink(c, rg);
  std::shared_ptr<arrow::io::OutputStream> sp(sink.get(), [](void*) {});
  ARROW_ASSIGN_OR_RAISE(auto w, arrow::ipc::MakeStreamWriter(sp, rbr->schema()));
  for (auto maybe : *rbr) {
    ARROW_ASSIGN_OR_RAISE(auto batch, maybe);
    ARROW_ASSIGN_OR_RAISE(auto t, arrow::Table::FromRecordBatches({batch}));
    ARROW_ASSIGN_OR_RAISE(auto mask, KeepMask(*t));
    ARROW_ASSIGN_OR_RAISE(arrow::Datum f, arrow::compute::Filter(batch, mask));
    ARROW_RETURN_NOT_OK(w->WriteRecordBatch(*f.record_batch()));
    c.s->rows_out += f.record_batch()->num_rows();
  }
  ARROW_RETURN_NOT_OK(w->Close());
  return sink->Close();
}

static arrow::Result<std::unique_ptr<Ctx>> OpenInput(Shared* s, const std::string& path) {
  auto c = std::make_unique<Ctx>();
  c->s = s;
  c->name = std::filesystem::path(path).stem().string();
  ARROW_ASSIGN_OR_RAISE(auto local, arrow::io::ReadableFile::Open(path));
  c->file = std::make_shared<SimGcsFile>(local, s->down);
  c->md = parquet::ReadMetaData(c->file);
  c->nrg = c->md->num_row_groups();
  if (s->mode != "transcode") return c;

  auto& tf = c->tf;
  tf.file = c->file;
  tf.md = c->md;
  auto pq = parquet::ParquetFileReader::Open(c->file, parquet::default_reader_properties(), c->md);
  auto page_index = pq->GetPageIndexReader();
  if (!page_index) return arrow::Status::NotImplemented(path, " has no page index");
  ARROW_ASSIGN_OR_RAISE(auto r, OpenReader(*c, 64 * 1024));
  ARROW_RETURN_NOT_OK(r->GetSchema(&tf.schema));
  tf.image_field = tf.schema->GetFieldIndex("image");
  tf.image_leaf = c->md->schema()->ColumnIndex("image");
  for (int i = 0; i < c->md->num_columns(); ++i)
    if (i != tf.image_leaf) tf.small_leaves.push_back(i);
  for (int rg = 0; rg < c->nrg; ++rg) {
    auto oidx = page_index->RowGroup(rg)->GetOffsetIndex(tf.image_leaf);
    if (!oidx) return arrow::Status::NotImplemented(path, " row group without offset index");
    tf.image_pages.push_back(oidx->page_locations());
  }
  return c;
}

// Work items are (file, row group) across all inputs. A worker that finds no
// row group ready opens the next file itself, outside the lock, so one file's
// startup overlaps other files' transfers instead of stalling everyone.
class WorkQueue {
 public:
  struct Item {
    Ctx* ctx;
    int rg;
  };
  WorkQueue(Shared* s, std::vector<std::string> paths) : s_(s), paths_(std::move(paths)) {}

  bool Next(Item* out) {
    std::unique_lock<std::mutex> lk(mu_);
    for (;;) {
      if (!items_.empty()) {
        *out = items_.front();
        items_.pop_front();
        return true;
      }
      if (next_file_ < paths_.size()) {
        std::string path = paths_[next_file_++];
        ++opening_;
        lk.unlock();
        auto r = OpenInput(s_, path);
        lk.lock();
        --opening_;
        cv_.notify_all();
        if (!r.ok()) {
          if (error.ok()) error = r.status();
          continue;
        }
        Ctx* c = r->get();
        open_.push_back(std::move(*r));
        for (int rg = 1; rg < c->nrg; ++rg) items_.push_back({c, rg});
        if (c->nrg == 0) continue;
        *out = {c, 0};
        return true;
      }
      if (opening_ == 0) return false;
      cv_.wait(lk);  // another worker is opening a file that may add items
    }
  }

  void Fail(const arrow::Status& st) {
    std::lock_guard<std::mutex> lk(mu_);
    if (error.ok()) error = st;
  }

  arrow::Status error;

 private:
  Shared* s_;
  std::vector<std::string> paths_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Item> items_;
  size_t next_file_ = 0;
  int opening_ = 0;
  std::vector<std::unique_ptr<Ctx>> open_;
};

static arrow::Status Verify(const std::string& a, const std::string& b) {
  int n = 0;
  for (auto& e : std::filesystem::directory_iterator(a)) {
    auto name = e.path().filename().string();
    auto read = [](const std::string& p) -> arrow::Result<std::shared_ptr<arrow::Table>> {
      ARROW_ASSIGN_OR_RAISE(auto f, arrow::io::ReadableFile::Open(p));
      ARROW_ASSIGN_OR_RAISE(auto r, arrow::ipc::RecordBatchStreamReader::Open(f));
      ARROW_ASSIGN_OR_RAISE(auto t, r->ToTable());
      return t->CombineChunks();
    };
    ARROW_ASSIGN_OR_RAISE(auto ta, read(e.path().string()));
    ARROW_ASSIGN_OR_RAISE(auto tb, read(b + "/" + name));
    if (!ta->Equals(*tb)) return arrow::Status::Invalid("mismatch in ", name);
    ++n;
  }
  std::printf("verify: %d objects identical\n", n);
  return arrow::Status::OK();
}

arrow::Status Main(int argc, char** argv) {
  ARROW_RETURN_NOT_OK(arrow::compute::Initialize());
  std::string mode = Arg(argc, argv, "mode", "transcode");
  if (mode == "verify") return Verify(Arg(argc, argv, "a", ""), Arg(argc, argv, "b", ""));

  std::string input = Arg(argc, argv, "dir", "");
  std::vector<std::string> paths;
  if (!input.empty()) {
    for (auto& e : std::filesystem::directory_iterator(input))
      if (e.path().extension() == ".parquet") paths.push_back(e.path().string());
    std::sort(paths.begin(), paths.end());
  } else {
    input = Arg(argc, argv, "file", "images.parquet");
    paths.push_back(input);
  }
  int64_t input_bytes = 0;
  for (auto& p : paths) input_bytes += std::filesystem::file_size(p);
  int workers = std::stoi(Arg(argc, argv, "workers", "4"));
  bool net_on = Arg(argc, argv, "net", "1") == "1";
  double latency_ms = std::stod(Arg(argc, argv, "latency-ms", "25"));
  double stream_mbps = std::stod(Arg(argc, argv, "stream-mbps", "150"));
  double global_mbps = std::stod(Arg(argc, argv, "global-mbps", "1500"));
  int64_t budget_mb = std::stoll(Arg(argc, argv, "budget-mb", "256"));
  int64_t unit_mb = std::stoll(Arg(argc, argv, "unit-mb", "8"));
  int inflight = std::stoi(Arg(argc, argv, "inflight", "4"));
  int64_t batch_rows = std::stoll(Arg(argc, argv, "batch-rows", "64"));
  int io_threads = std::stoi(Arg(argc, argv, "io-threads", "16"));
  int up_threads = std::stoi(Arg(argc, argv, "upload-threads", "8"));

  ARROW_RETURN_NOT_OK(arrow::io::SetIOThreadPoolCapacity(io_threads));
  NetModel down(net_on, latency_ms / 1e3, stream_mbps * 1e6, global_mbps * 1e6);
  NetModel up(net_on, latency_ms / 1e3, stream_mbps * 1e6, global_mbps * 1e6);
  Uploader uploader(up_threads, 2 * up_threads, &up);

  Shared sh;
  sh.mode = mode;
  sh.out_dir = Arg(argc, argv, "out-dir", "");
  if (!sh.out_dir.empty()) std::filesystem::create_directories(sh.out_dir);
  sh.uploader = &uploader;
  sh.down = &down;

  MemSampler mem;
  double t0 = NowSec();
  ByteBudget budget(budget_mb << 20);
  TranscodeOptions topts{unit_mb << 20, inflight};
  WorkQueue queue(&sh, paths);

  std::vector<std::thread> ts;
  for (int w = 0; w < workers; ++w) {
    ts.emplace_back([&] {
      for (WorkQueue::Item it; queue.Next(&it);) {
        Ctx& c = *it.ctx;
        arrow::Status st;
        if (mode == "table") {
          st = RunTable(c, it.rg);
        } else if (mode == "stream") {
          st = RunStream(c, it.rg, batch_rows);
        } else {
          auto small_reader = OpenReader(c, 64 * 1024);
          if (!small_reader.ok()) {
            queue.Fail(small_reader.status());
            continue;
          }
          RowGroupTranscoder t(c.tf, it.rg, &budget, topts);
          auto sink = MakeSink(c, it.rg);
          st = t.Run(small_reader->get(), KeepRows, sink.get());
          sh.rows_out += t.rows_out();
        }
        if (!st.ok()) queue.Fail(st);
      }
    });
  }
  for (auto& t : ts) t.join();
  uploader.Drain();
  double wall = NowSec() - t0;
  mem.Stop();
  ARROW_RETURN_NOT_OK(queue.error);

  auto* pool = arrow::default_memory_pool();
  std::printf(
      "{\"input\":\"%s\",\"files\":%zu,\"mode\":\"%s\",\"net\":%d,\"workers\":%d,\"budget_mb\":%lld,\"upload_threads\":%d,\"wall_s\":%.2f,\"input_mb\":%.0f,"
      "\"throughput_mbps\":%.0f,\"rows_out\":%lld,\"get_requests\":%lld,\"get_mb\":%.0f,"
      "\"put_mb\":%.0f,\"peak_footprint_mb\":%.0f,\"peak_rss_mb\":%.0f,\"arrow_pool_peak_mb\":%.0f,"
      "\"budget_peak_mb\":%.0f,\"ipc_by_ref_mb\":%.0f,\"ipc_copied_mb\":%.1f,\"pool\":\"%s\"}\n",
      input.c_str(), paths.size(), mode.c_str(), net_on, workers, (long long)budget_mb, up_threads, wall, input_bytes / 1e6, input_bytes / 1e6 / wall,
      (long long)sh.rows_out.load(), (long long)down.requests.load(), down.bytes.load() / 1e6,
      up.bytes.load() / 1e6, mem.peak() / 1e6, PeakRss() / 1e6, pool->max_memory() / 1e6,
      budget.peak() / 1e6, PartOutputStream::by_ref_bytes.load() / 1e6,
      PartOutputStream::copied_bytes.load() / 1e6, pool->backend_name().c_str());
  auto span = [](NetModel& n) { return n.last_end > n.first_start ? n.last_end - n.first_start : 0.0; };
  std::fprintf(stderr,
      "  detail: upload_threads=%d down{busy=%.1fs span=%.2fs avg_streams=%.1f} up{busy=%.1fs span=%.2fs avg_streams=%.1f first_start=+%.2fs}\n"
      "  phases (worker-s): setup=%.2f budget_wait=%.2f read_wait=%.2f decode=%.2f write=%.2f\n",
      up_threads, down.busy_us / 1e6, span(down), span(down) > 0 ? down.busy_us / 1e6 / span(down) : 0,
      up.busy_us / 1e6, span(up), span(up) > 0 ? up.busy_us / 1e6 / span(up) : 0, up.first_start - t0,
      g_phase.setup_us / 1e6, g_phase.budget_wait_us / 1e6, g_phase.read_wait_us / 1e6,
      g_phase.decode_us / 1e6, g_phase.write_us / 1e6);
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  auto st = Main(argc, argv);
  if (!st.ok()) {
    std::fprintf(stderr, "error: %s\n", st.ToString().c_str());
    return 1;
  }
}
