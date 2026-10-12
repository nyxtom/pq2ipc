// Page-level Parquet -> Arrow IPC transcoder with a global byte budget.
//
// The large binary column (images) is never materialized through
// parquet::arrow. Its pages are fetched in coalesced byte ranges, decoded to
// (ptr, len) views over the decompressed page, and copied once into an exactly
// sized values buffer. That buffer carries a budget lease that is released only
// when the uploader drops it, so bytes in flight across fetch, decode, the IPC
// writer and upload stay under one cap.
#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <parquet/encoding.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/page_index.h>

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>

// ---------------------------------------------------------------- budget

class ByteBudget {
 public:
  explicit ByteBudget(int64_t cap) : cap_(cap) {}

  bool TryAcquire(int64_t n) {
    std::lock_guard<std::mutex> lk(mu_);
    if (used_ + n > cap_ && used_ != 0) return false;
    Take(n);
    return true;
  }
  void Acquire(int64_t n) {
    std::unique_lock<std::mutex> lk(mu_);
    // An item larger than the whole budget is let through alone.
    cv_.wait(lk, [&] { return used_ + n <= cap_ || used_ == 0; });
    Take(n);
  }
  // Accounting adjustment for memory that already exists; never blocks.
  void ForceAcquire(int64_t n) {
    std::lock_guard<std::mutex> lk(mu_);
    Take(n);
  }
  void Release(int64_t n) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      used_ -= n;
    }
    cv_.notify_all();
  }
  int64_t peak() const { return peak_; }

 private:
  void Take(int64_t n) {
    used_ += n;
    peak_ = std::max(peak_, used_);
  }
  std::mutex mu_;
  std::condition_variable cv_;
  int64_t cap_, used_ = 0, peak_ = 0;
};

struct Lease {
  Lease(ByteBudget* b, int64_t n) : budget(b), held(n) {}
  ~Lease() { budget->Release(held); }
  // Re-point the lease at the buffer that now holds the bytes.
  void ResizeTo(int64_t n) {
    if (n < held) budget->Release(held - n);
    if (n > held) budget->ForceAcquire(n - held);
    held = n;
  }
  ByteBudget* budget;
  int64_t held;
};

class LeasedBuffer : public arrow::Buffer {
 public:
  LeasedBuffer(std::shared_ptr<arrow::Buffer> inner, std::shared_ptr<Lease> lease)
      : arrow::Buffer(inner, 0, inner->size()), lease_(std::move(lease)) {}

 private:
  std::shared_ptr<Lease> lease_;
};

// ---------------------------------------------------------------- output

// Collects buffer references (not copies) and hands them off in parts of
// roughly `part_size` bytes. Small writes (IPC headers, padding) are copied.
class PartOutputStream : public arrow::io::OutputStream {
 public:
  using Sink = std::function<arrow::Status(std::vector<std::shared_ptr<arrow::Buffer>>)>;
  static inline std::atomic<int64_t> by_ref_bytes{0}, copied_bytes{0};

  PartOutputStream(int64_t part_size, Sink sink) : part_size_(part_size), sink_(std::move(sink)) {}

  arrow::Status Write(const std::shared_ptr<arrow::Buffer>& b) override {
    by_ref_bytes += b->size();
    return Append(b);
  }
  arrow::Status Write(const void* data, int64_t n) override {
    copied_bytes += n;
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> b, arrow::AllocateBuffer(n));
    std::memcpy(b->mutable_data(), data, n);
    return Append(b);
  }
  arrow::Status Flush() override {
    if (pending_.empty()) return arrow::Status::OK();
    pending_bytes_ = 0;
    return sink_(std::exchange(pending_, {}));
  }
  arrow::Status Close() override {
    ARROW_RETURN_NOT_OK(Flush());
    closed_ = true;
    return arrow::Status::OK();
  }
  bool closed() const override { return closed_; }
  arrow::Result<int64_t> Tell() const override { return pos_; }

 private:
  arrow::Status Append(std::shared_ptr<arrow::Buffer> b) {
    pos_ += b->size();
    pending_bytes_ += b->size();
    pending_.push_back(std::move(b));
    return pending_bytes_ >= part_size_ ? Flush() : arrow::Status::OK();
  }
  int64_t part_size_, pending_bytes_ = 0, pos_ = 0;
  bool closed_ = false;
  std::vector<std::shared_ptr<arrow::Buffer>> pending_;
  Sink sink_;
};

// ---------------------------------------------------------------- transcoder

struct TranscodeOptions {
  int64_t unit_bytes = 8 << 20;  // coalesce adjacent pages into ranged reads of this size
  int max_inflight_units = 4;    // per row group
};

// Shared, read-only state for one input file.
struct TranscodeFile {
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  std::shared_ptr<parquet::FileMetaData> md;
  // Offset index of the image column per row group, loaded once up front:
  // PageIndexReader is not safe to share across threads.
  std::vector<std::vector<parquet::PageLocation>> image_pages;
  std::shared_ptr<arrow::Schema> schema;  // output schema, same as the source
  int image_leaf = -1;                    // parquet leaf column index
  int image_field = -1;                   // arrow field index
  std::vector<int> small_leaves;          // everything else
};

// Returns per-row keep flags for a row group, computed from the small columns.
using KeepFn = std::function<arrow::Result<std::vector<uint8_t>>(const arrow::Table& small)>;

namespace detail {

struct Unit {
  int first_page, end_page;
  int64_t offset, len;
  int64_t row0, row1;
};

struct DecodedPage {
  std::shared_ptr<parquet::Page> page;  // keeps the decompressed bytes alive
  int64_t row0, row1;
  std::vector<int16_t> def;  // empty when the column is required
  std::vector<parquet::ByteArray> vals;
};

}  // namespace detail

// Worker-seconds spent in each phase, summed across workers.
struct PhaseTimes {
  std::atomic<int64_t> setup_us{0}, budget_wait_us{0}, read_wait_us{0}, decode_us{0}, write_us{0};
};
inline PhaseTimes g_phase;

struct ScopedTimer {
  explicit ScopedTimer(std::atomic<int64_t>& acc)
      : acc_(acc), t0_(std::chrono::steady_clock::now()) {}
  ~ScopedTimer() {
    acc_ += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0_).count();
  }
  std::atomic<int64_t>& acc_;
  std::chrono::steady_clock::time_point t0_;
};

class RowGroupTranscoder {
 public:
  RowGroupTranscoder(const TranscodeFile& f, int rg, ByteBudget* budget, TranscodeOptions opts)
      : f_(f), rg_(rg), budget_(budget), opts_(opts) {}

  arrow::Status Run(parquet::arrow::FileReader* small_reader, const KeepFn& keep_fn,
                    PartOutputStream* sink) {
    auto rgmd = f_.md->RowGroup(rg_);
    rg_rows_ = rgmd->num_rows();
    descr_ = f_.md->schema()->Column(f_.image_leaf);
    max_def_ = descr_->max_definition_level();
    if (descr_->max_repetition_level() != 0) return arrow::Status::NotImplemented("nested image column");
    auto cc = rgmd->ColumnChunk(f_.image_leaf);
    codec_ = cc->compression();
    ratio_ = std::max(1.0, (double)cc->total_uncompressed_size() / cc->total_compressed_size());

    std::optional<ScopedTimer> setup_timer(std::in_place, g_phase.setup_us);
    // Start the dictionary page download now so it overlaps the small-column read.
    arrow::Future<std::shared_ptr<arrow::Buffer>> dict_fut;
    if (cc->has_dictionary_page()) {
      int64_t off = cc->dictionary_page_offset();
      dict_fut = f_.file->ReadAsync(arrow::io::default_io_context(), off, cc->data_page_offset() - off);
    }
    // 1. Small columns: read whole, filter.
    std::shared_ptr<arrow::Table> small;
    ARROW_RETURN_NOT_OK(small_reader->ReadRowGroup(rg_, f_.small_leaves, &small));
    ARROW_ASSIGN_OR_RAISE(small_, small->CombineChunks());
    ARROW_ASSIGN_OR_RAISE(keep_, keep_fn(*small_));
    {
      arrow::BooleanBuilder bb;
      ARROW_RETURN_NOT_OK(bb.Reserve(rg_rows_));
      for (auto k : keep_) bb.UnsafeAppend(k != 0);
      ARROW_RETURN_NOT_OK(bb.Finish(&keep_arr_));
    }

    // 2. Dictionary page (writers often start with dictionary encoding and
    //    fall back to PLAIN once the dictionary gets too large).
    plain_ = parquet::MakeTypedDecoder<parquet::ByteArrayType>(parquet::Encoding::PLAIN, descr_);
    // The dictionary is per-worker scratch like the decode buffers, not
    // budgeted: a worker must never hold budget while it waits on the budget,
    // or workers can deadlock each holding a share.
    if (cc->has_dictionary_page()) {
      ARROW_ASSIGN_OR_RAISE(auto raw, dict_fut.result());
      auto pr = parquet::PageReader::Open(std::make_shared<arrow::io::BufferReader>(raw), 1, codec_,
                                          parquet::default_reader_properties(), *descr_);
      dict_page_ = pr->NextPage();
      if (!dict_page_ || dict_page_->type() != parquet::PageType::DICTIONARY_PAGE)
        return arrow::Status::Invalid("expected dictionary page");
      auto* dp = static_cast<parquet::DictionaryPage*>(dict_page_.get());
      auto dict_plain = parquet::MakeTypedDecoder<parquet::ByteArrayType>(parquet::Encoding::PLAIN, descr_);
      dict_plain->SetData(dp->num_values(), dp->data(), (int)dp->size());
      dict_views_.resize(dp->num_values());
      dict_plain->Decode(dict_views_.data(), dp->num_values());
      dict_plain->SetData(dp->num_values(), dp->data(), (int)dp->size());
      dict_ = parquet::MakeDictDecoder<parquet::ByteArrayType>(descr_);
      dict_->SetDict(dict_plain.get());
    }

    // 3. Plan coalesced ranged reads over pages that contain kept rows.
    locs_ = f_.image_pages[rg_];
    PlanUnits();
    setup_timer.reset();

    // 4. Pipeline: keep up to N ranged reads in flight, decode and write in order.
    arrow::ipc::IpcWriteOptions wopts = arrow::ipc::IpcWriteOptions::Defaults();
    std::shared_ptr<arrow::io::OutputStream> sink_ptr(sink, [](void*) {});
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(sink_ptr, f_.schema, wopts));

    struct InFlight {
      size_t unit;
      arrow::Future<std::shared_ptr<arrow::Buffer>> fut;
      std::shared_ptr<Lease> lease;
    };
    std::deque<InFlight> q;
    size_t next = 0;
    auto io = arrow::io::default_io_context();
    while (next < units_.size() || !q.empty()) {
      while (next < units_.size() && (int)q.size() < opts_.max_inflight_units) {
        // Only the raw range is held while in flight. Decode scratch
        // (decompressed pages) is per worker, outside the budget.
        int64_t reserve = units_[next].len;
        if (!budget_->TryAcquire(reserve)) {
          if (!q.empty()) break;  // make progress on what we already hold
          // Nothing in flight: push our buffered output to the uploader so
          // we never wait on bytes we ourselves are holding.
          ScopedTimer t(g_phase.budget_wait_us);
          ARROW_RETURN_NOT_OK(sink->Flush());
          budget_->Acquire(reserve);
        }
        auto lease = std::make_shared<Lease>(budget_, reserve);
        q.push_back({next, f_.file->ReadAsync(io, units_[next].offset, units_[next].len), lease});
        ++next;
      }
      InFlight cur = std::move(q.front());
      q.pop_front();
      std::shared_ptr<arrow::Buffer> raw;
      {
        ScopedTimer t(g_phase.read_wait_us);
        ARROW_ASSIGN_OR_RAISE(raw, cur.fut.result());
      }
      std::shared_ptr<arrow::RecordBatch> batch;
      {
        ScopedTimer t(g_phase.decode_us);
        ARROW_ASSIGN_OR_RAISE(batch, DecodeUnit(units_[cur.unit], std::move(raw), cur.lease));
      }
      ScopedTimer t(g_phase.write_us);  // includes blocking on a full upload queue
      ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    }
    ScopedTimer t(g_phase.write_us);
    ARROW_RETURN_NOT_OK(writer->Close());
    return sink->Close();
  }

  int64_t rows_out() const { return rows_out_; }

 private:
  void PlanUnits() {
    const int n = (int)locs_.size();
    for (int i = 0; i < n; ++i) {
      int64_t r0 = locs_[i].first_row_index;
      int64_t r1 = i + 1 < n ? locs_[i + 1].first_row_index : rg_rows_;
      bool any = false;
      for (int64_t r = r0; r < r1 && !any; ++r) any = keep_[r];
      if (!any) continue;  // never downloaded
      int64_t size = locs_[i].compressed_page_size;
      if (!units_.empty()) {
        auto& u = units_.back();
        if (u.end_page == i && u.offset + u.len == locs_[i].offset && u.len + size <= opts_.unit_bytes) {
          u.end_page = i + 1;
          u.len += size;
          u.row1 = r1;
          continue;
        }
      }
      units_.push_back({i, i + 1, locs_[i].offset, size, r0, r1});
    }
  }

  arrow::Result<detail::DecodedPage> DecodePage(int p, const std::shared_ptr<arrow::Buffer>& unit_buf,
                                                int64_t unit_offset) {
    detail::DecodedPage out;
    out.row0 = locs_[p].first_row_index;
    out.row1 = p + 1 < (int)locs_.size() ? locs_[p + 1].first_row_index : rg_rows_;
    int64_t rows = out.row1 - out.row0;
    auto slice = arrow::SliceBuffer(unit_buf, locs_[p].offset - unit_offset, locs_[p].compressed_page_size);
    auto pr = parquet::PageReader::Open(std::make_shared<arrow::io::BufferReader>(slice), rows, codec_,
                                        parquet::default_reader_properties(), *descr_);
    out.page = pr->NextPage();
    if (!out.page) return arrow::Status::Invalid("missing page");
    auto* dp = static_cast<parquet::DataPage*>(out.page.get());
    const uint8_t* data = dp->data();
    int64_t size = dp->size();
    int nv = dp->num_values();
    if (nv != rows) return arrow::Status::Invalid("page value count != row count");

    if (dp->type() == parquet::PageType::DATA_PAGE) {
      if (max_def_ > 0) {
        auto* v1 = static_cast<parquet::DataPageV1*>(dp);
        parquet::LevelDecoder ld;
        int consumed = ld.SetData(v1->definition_level_encoding(), max_def_, nv, data, (int32_t)size);
        out.def.resize(nv);
        ld.Decode(nv, out.def.data());
        data += consumed;
        size -= consumed;
      }
    } else if (dp->type() == parquet::PageType::DATA_PAGE_V2) {
      auto* v2 = static_cast<parquet::DataPageV2*>(dp);
      int32_t rep_len = v2->repetition_levels_byte_length();
      int32_t def_len = v2->definition_levels_byte_length();
      if (max_def_ > 0) {
        parquet::LevelDecoder ld;
        ld.SetDataV2(def_len, max_def_, nv, data + rep_len);
        out.def.resize(nv);
        ld.Decode(nv, out.def.data());
      }
      data += rep_len + def_len;
      size -= rep_len + def_len;
    } else {
      return arrow::Status::Invalid("unexpected page type");
    }

    int non_null = nv;
    if (max_def_ > 0) non_null = (int)std::count(out.def.begin(), out.def.end(), max_def_);
    out.vals.resize(non_null);
    switch (dp->encoding()) {
      case parquet::Encoding::PLAIN:
        plain_->SetData(non_null, data, (int)size);
        plain_->Decode(out.vals.data(), non_null);
        break;
      case parquet::Encoding::RLE_DICTIONARY:
      case parquet::Encoding::PLAIN_DICTIONARY: {
        if (!dict_) return arrow::Status::Invalid("dictionary page missing");
        idx_.resize(non_null);
        dict_->SetData(non_null, data, (int)size);
        dict_->DecodeIndices(non_null, idx_.data());
        for (int i = 0; i < non_null; ++i) out.vals[i] = dict_views_[idx_[i]];
        break;
      }
      default:
        return arrow::Status::NotImplemented("image column encoding ",
                                             parquet::EncodingToString(dp->encoding()));
    }
    return out;
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> DecodeUnit(const detail::Unit& u,
                                                                std::shared_ptr<arrow::Buffer> raw,
                                                                const std::shared_ptr<Lease>& lease) {
    std::shared_ptr<arrow::Array> image;
    {
      std::vector<detail::DecodedPage> pages;
      for (int p = u.first_page; p < u.end_page; ++p) {
        ARROW_ASSIGN_OR_RAISE(auto dp, DecodePage(p, raw, u.offset));
        pages.push_back(std::move(dp));
      }

      int64_t kept = 0, total = 0, nulls = 0;
      for (auto& pg : pages) {
        int64_t vi = 0;
        for (int64_t r = pg.row0; r < pg.row1; ++r) {
          bool valid = pg.def.empty() || pg.def[r - pg.row0] == max_def_;
          if (keep_[r]) {
            ++kept;
            if (valid) total += pg.vals[vi].len;
            else ++nulls;
          }
          if (valid) ++vi;
        }
      }
      if (total > std::numeric_limits<int32_t>::max()) return arrow::Status::CapacityError("unit > 2GB");

      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> offsets,
                            arrow::AllocateBuffer((kept + 1) * sizeof(int32_t)));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> values, arrow::AllocateBuffer(total));
      std::shared_ptr<arrow::Buffer> validity;
      uint8_t* vbits = nullptr;
      if (nulls > 0) {
        ARROW_ASSIGN_OR_RAISE(validity, arrow::AllocateEmptyBitmap(kept));
        vbits = validity->mutable_data();
      }
      auto* off = reinterpret_cast<int32_t*>(offsets->mutable_data());
      uint8_t* dst = values->mutable_data();
      int32_t pos = 0;
      int64_t j = 0;
      off[0] = 0;
      for (auto& pg : pages) {
        int64_t vi = 0;
        for (int64_t r = pg.row0; r < pg.row1; ++r) {
          bool valid = pg.def.empty() || pg.def[r - pg.row0] == max_def_;
          if (keep_[r]) {
            if (valid) {
              const auto& v = pg.vals[vi];
              std::memcpy(dst + pos, v.ptr, v.len);  // the one copy per image
              pos += v.len;
              if (vbits) arrow::bit_util::SetBit(vbits, j);
            }
            off[++j] = pos;
          }
          if (valid) ++vi;
        }
      }
      auto leased = std::make_shared<LeasedBuffer>(std::move(values), lease);
      image = arrow::MakeArray(arrow::ArrayData::Make(arrow::binary(), kept,
                                                      {validity, offsets, leased}, nulls));
      // `pages` and `raw` die here; only the compacted values stay leased.
    }
    raw.reset();
    lease->ResizeTo(image->data()->buffers[2]->size());

    // Small columns for the same row range.
    int64_t n = u.row1 - u.row0;
    ARROW_ASSIGN_OR_RAISE(arrow::Datum filtered,
                          arrow::compute::Filter(small_->Slice(u.row0, n), keep_arr_->Slice(u.row0, n)));
    auto small = filtered.table();
    std::vector<std::shared_ptr<arrow::Array>> cols;
    for (int i = 0; i < f_.schema->num_fields(); ++i) {
      if (i == f_.image_field) {
        cols.push_back(image);
      } else {
        auto c = small->GetColumnByName(f_.schema->field(i)->name());
        if (c->num_chunks() == 1) {
          cols.push_back(c->chunk(0));
        } else {
          ARROW_ASSIGN_OR_RAISE(auto a, arrow::Concatenate(c->chunks()));
          cols.push_back(a);
        }
      }
    }
    rows_out_ += image->length();
    return arrow::RecordBatch::Make(f_.schema, image->length(), std::move(cols));
  }

  const TranscodeFile& f_;
  int rg_;
  ByteBudget* budget_;
  TranscodeOptions opts_;

  int64_t rg_rows_ = 0, rows_out_ = 0;
  const parquet::ColumnDescriptor* descr_ = nullptr;
  int16_t max_def_ = 0;
  arrow::Compression::type codec_;
  double ratio_ = 1;

  std::shared_ptr<arrow::Table> small_;
  std::vector<uint8_t> keep_;
  std::shared_ptr<arrow::Array> keep_arr_;

  std::unique_ptr<parquet::TypedDecoder<parquet::ByteArrayType>> plain_;
  std::unique_ptr<parquet::DictDecoder<parquet::ByteArrayType>> dict_;
  std::shared_ptr<parquet::Page> dict_page_;
  std::vector<parquet::ByteArray> dict_views_;
  std::vector<int32_t> idx_;

  std::vector<parquet::PageLocation> locs_;
  std::vector<detail::Unit> units_;
};
