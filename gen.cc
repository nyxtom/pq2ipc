// Generates an image-table Parquet file using Arrow's default writer settings
// (dictionary enabled with fallback, V1 data pages) plus a page index.
//
//   gen --out=images.parquet --rows=6000 --rg-rows=750 --codec=snappy
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include <cstdio>
#include <cstring>
#include <random>
#include <string>

static std::string Arg(int argc, char** argv, const char* name, const char* def) {
  std::string prefix = std::string("--") + name + "=";
  for (int i = 1; i < argc; ++i)
    if (std::strncmp(argv[i], prefix.c_str(), prefix.size()) == 0) return argv[i] + prefix.size();
  return def;
}

arrow::Status Run(int argc, char** argv) {
  std::string out = Arg(argc, argv, "out", "images.parquet");
  int64_t rows = std::stoll(Arg(argc, argv, "rows", "6000"));
  int64_t rg_rows = std::stoll(Arg(argc, argv, "rg-rows", "750"));
  std::string codec_name = Arg(argc, argv, "codec", "snappy");
  int64_t min_kb = std::stoll(Arg(argc, argv, "min-kb", "50"));
  int64_t max_kb = std::stoll(Arg(argc, argv, "max-kb", "450"));
  uint64_t seed = std::stoull(Arg(argc, argv, "seed", "42"));

  auto codec = codec_name == "zstd"   ? arrow::Compression::ZSTD
               : codec_name == "none" ? arrow::Compression::UNCOMPRESSED
                                      : arrow::Compression::SNAPPY;

  // Image column sits in the middle so the IPC body ordering is exercised.
  auto schema = arrow::schema({
      arrow::field("id", arrow::int64(), false),
      arrow::field("label", arrow::utf8()),
      arrow::field("image", arrow::binary()),
      arrow::field("width", arrow::int32()),
      arrow::field("height", arrow::int32()),
  });

  auto props = parquet::WriterProperties::Builder()
                   .compression(codec)
                   ->max_row_group_length(rg_rows)
                   ->enable_write_page_index()
                   ->build();
  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::FileOutputStream::Open(out));
  ARROW_ASSIGN_OR_RAISE(auto writer, parquet::arrow::FileWriter::Open(
                                         *schema, arrow::default_memory_pool(), sink, props,
                                         parquet::ArrowWriterProperties::Builder().store_schema()->build()));

  std::mt19937_64 rng(seed);
  std::uniform_int_distribution<int64_t> size_dist(min_kb * 1024, max_kb * 1024);
  const int64_t kBatch = 50;
  int64_t total_bytes = 0;
  for (int64_t start = 0; start < rows; start += kBatch) {
    int64_t n = std::min(kBatch, rows - start);
    arrow::Int64Builder id_b;
    arrow::StringBuilder label_b;
    arrow::BinaryBuilder img_b;
    arrow::Int32Builder w_b, h_b;
    for (int64_t i = 0; i < n; ++i) {
      int64_t id = (int64_t)seed * 1000000 + start + i;
      ARROW_RETURN_NOT_OK(id_b.Append(id));
      ARROW_RETURN_NOT_OK(label_b.Append("class_" + std::to_string(id % 37)));
      if (id % 97 == 13) {  // ~1% null images
        ARROW_RETURN_NOT_OK(img_b.AppendNull());
      } else {
        // Random bytes behave like JPEG/PNG payloads: incompressible.
        int64_t sz = size_dist(rng);
        std::string blob(sz, '\0');
        auto* p = reinterpret_cast<uint64_t*>(blob.data());
        for (int64_t k = 0; k < sz / 8; ++k) p[k] = rng();
        ARROW_RETURN_NOT_OK(img_b.Append(blob));
        total_bytes += sz;
      }
      ARROW_RETURN_NOT_OK(w_b.Append(640 + id % 7));
      ARROW_RETURN_NOT_OK(h_b.Append(480 + id % 5));
    }
    std::shared_ptr<arrow::Array> a, b, c, d, e;
    ARROW_RETURN_NOT_OK(id_b.Finish(&a));
    ARROW_RETURN_NOT_OK(label_b.Finish(&b));
    ARROW_RETURN_NOT_OK(img_b.Finish(&c));
    ARROW_RETURN_NOT_OK(w_b.Finish(&d));
    ARROW_RETURN_NOT_OK(h_b.Finish(&e));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*arrow::RecordBatch::Make(schema, n, {a, b, c, d, e})));
  }
  ARROW_RETURN_NOT_OK(writer->Close());
  ARROW_RETURN_NOT_OK(sink->Close());
  std::printf("wrote %s: %lld rows, %.1f MB of image payload, codec=%s\n", out.c_str(),
              (long long)rows, total_bytes / 1e6, codec_name.c_str());
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  auto st = Run(argc, argv);
  if (!st.ok()) {
    std::fprintf(stderr, "%s\n", st.ToString().c_str());
    return 1;
  }
}
