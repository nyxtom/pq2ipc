// Benchmark scaffolding: a simulated GCS link, a range-read file wrapper over
// it, a bounded parallel uploader, and process memory probes.
#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <mach/mach.h>
#include <sys/resource.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

inline double NowSec() {
  return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Per-request latency, per-stream bandwidth, and a shared aggregate pipe.
class NetModel {
 public:
  NetModel(bool enabled, double latency_s, double stream_bps, double global_bps)
      : enabled_(enabled), latency_(latency_s), stream_bps_(stream_bps), global_bps_(global_bps) {}

  void Transfer(int64_t bytes) {
    requests.fetch_add(1);
    this->bytes.fetch_add(bytes);
    if (!enabled_) return;
    double now = NowSec();
    double pipe_done;
    {
      std::lock_guard<std::mutex> lk(mu_);
      double start = std::max(now, next_free_);
      next_free_ = start + bytes / global_bps_;
      pipe_done = next_free_;
    }
    double finish = std::max(now + latency_ + bytes / stream_bps_, pipe_done + latency_);
    double wait = finish - NowSec();
    if (wait > 0) std::this_thread::sleep_for(std::chrono::duration<double>(wait));
    double end = NowSec();
    busy_us.fetch_add((int64_t)((end - now) * 1e6));
    std::lock_guard<std::mutex> lk(mu_);
    first_start = std::min(first_start, now);
    last_end = std::max(last_end, end);
  }

  std::atomic<int64_t> requests{0}, bytes{0};
  std::atomic<int64_t> busy_us{0};  // sum of per-request durations
  double first_start = 1e300, last_end = 0;

 private:
  bool enabled_;
  double latency_, stream_bps_, global_bps_;
  std::mutex mu_;
  double next_free_ = 0;
};

// A local file where every range read pays the simulated network cost, like a
// GCS ranged GET. ReadAsync uses the base class, which runs ReadAt on Arrow's
// IO thread pool.
class SimGcsFile : public arrow::io::RandomAccessFile {
 public:
  SimGcsFile(std::shared_ptr<arrow::io::RandomAccessFile> inner, NetModel* net)
      : inner_(std::move(inner)), net_(net) {}

  arrow::Status Close() override { return inner_->Close(); }
  bool closed() const override { return inner_->closed(); }
  arrow::Result<int64_t> Tell() const override { return pos_; }
  arrow::Status Seek(int64_t p) override {
    pos_ = p;
    return arrow::Status::OK();
  }
  arrow::Result<int64_t> GetSize() override { return inner_->GetSize(); }

  arrow::Result<int64_t> Read(int64_t n, void* out) override {
    ARROW_ASSIGN_OR_RAISE(int64_t r, ReadAt(pos_, n, out));
    pos_ += r;
    return r;
  }
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t n) override {
    ARROW_ASSIGN_OR_RAISE(auto b, ReadAt(pos_, n));
    pos_ += b->size();
    return b;
  }
  arrow::Result<int64_t> ReadAt(int64_t p, int64_t n, void* out) override {
    ARROW_ASSIGN_OR_RAISE(int64_t r, inner_->ReadAt(p, n, out));
    net_->Transfer(r);
    return r;
  }
  arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t p, int64_t n) override {
    ARROW_ASSIGN_OR_RAISE(auto b, inner_->ReadAt(p, n));
    net_->Transfer(b->size());
    return b;
  }

 private:
  std::shared_ptr<arrow::io::RandomAccessFile> inner_;
  NetModel* net_;
  int64_t pos_ = 0;
};

// Uploads parts on a fixed number of threads. Submit blocks while `queue_cap`
// parts are waiting, so a slow link pushes back on producers instead of
// queueing without limit. Buffers are dropped (and any leases released) once
// the part "lands".
class Uploader {
 public:
  using Part = std::vector<std::shared_ptr<arrow::Buffer>>;

  Uploader(int threads, int queue_cap, NetModel* net) : cap_(queue_cap), net_(net) {
    for (int i = 0; i < threads; ++i) threads_.emplace_back([this] { Loop(); });
  }
  ~Uploader() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      stop_ = true;
    }
    cv_.notify_all();
    for (auto& t : threads_) t.join();
  }

  void Submit(Part part) {
    std::unique_lock<std::mutex> lk(mu_);
    space_cv_.wait(lk, [&] { return (int)queue_.size() < cap_; });
    queue_.push_back(std::move(part));
    ++outstanding_;
    cv_.notify_one();
  }

  void Drain() {
    std::unique_lock<std::mutex> lk(mu_);
    done_cv_.wait(lk, [&] { return outstanding_ == 0; });
  }

 private:
  void Loop() {
    for (;;) {
      Part part;
      {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&] { return stop_ || !queue_.empty(); });
        if (stop_ && queue_.empty()) return;
        part = std::move(queue_.front());
        queue_.pop_front();
        space_cv_.notify_one();
      }
      int64_t n = 0;
      for (auto& b : part) n += b->size();
      net_->Transfer(n);
      part.clear();  // last reference for transcoder buffers -> lease released
      {
        std::lock_guard<std::mutex> lk(mu_);
        if (--outstanding_ == 0) done_cv_.notify_all();
      }
    }
  }

  int cap_;
  NetModel* net_;
  std::mutex mu_;
  std::condition_variable cv_, space_cv_, done_cv_;
  std::deque<Part> queue_;
  int64_t outstanding_ = 0;
  bool stop_ = false;
  std::vector<std::thread> threads_;
};

// Physical footprint is what Activity Monitor / jetsam use on macOS; sample it
// in the background to catch the peak.
inline int64_t PhysFootprint() {
  task_vm_info_data_t info;
  mach_msg_type_number_t count = TASK_VM_INFO_COUNT;
  if (task_info(mach_task_self(), TASK_VM_INFO, (task_info_t)&info, &count) != KERN_SUCCESS) return 0;
  return (int64_t)info.phys_footprint;
}

inline int64_t PeakRss() {
  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);
  return ru.ru_maxrss;  // bytes on macOS
}

class MemSampler {
 public:
  MemSampler() : t_([this] {
      while (!stop_) {
        peak_ = std::max<int64_t>(peak_, PhysFootprint());
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
    }) {}
  ~MemSampler() { Stop(); }
  void Stop() {
    if (!stop_.exchange(true)) t_.join();
  }
  int64_t peak() const { return peak_; }

 private:
  std::atomic<bool> stop_{false};
  std::atomic<int64_t> peak_{0};
  std::thread t_;
};
