#pragma once
#include <deque>
#include <stdexcept>
#include <string>
#include <vector>

// 每个 worker 独占；失败的 Remove 留在窗口内，不能被新 Put 淹没。
class LiveWindow {
   public:
    explicit LiveWindow(size_t limit) : limit_(limit) {
        if (!limit_) throw std::invalid_argument("存量窗口必须为正数");
    }
    void Add(std::string key) { keys_.push_back(std::move(key)); }
    size_t size() const { return keys_.size(); }
    std::vector<std::string> Due(size_t batch) const {
        if (keys_.size() < limit_) return {};
        size_t count = std::min(batch, keys_.size());
        return {keys_.begin(), keys_.begin() + count};
    }
    void Complete(const std::vector<bool>& succeeded) {
        if (succeeded.size() > keys_.size())
            throw std::invalid_argument("Remove 结果越界");
        std::deque<std::string> retry;
        for (bool ok : succeeded) {
            if (!ok) retry.push_back(std::move(keys_.front()));
            keys_.pop_front();
        }
        for (auto it = retry.rbegin(); it != retry.rend(); ++it)
            keys_.push_front(std::move(*it));
    }

   private:
    size_t limit_;
    std::deque<std::string> keys_;
};
