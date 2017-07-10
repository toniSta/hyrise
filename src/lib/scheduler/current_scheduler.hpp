#pragma once

#include <memory>
#include <vector>

#include "abstract_scheduler.hpp"
#include "utils/assert.hpp"
#include "worker.hpp"

namespace opossum {

class AbstractTask;

/**
 * Holds the singleton instance (or the lack of one) of the currently active Scheduler
 */
class CurrentScheduler {
 public:
  static const std::shared_ptr<AbstractScheduler>& get();
  static void set(const std::shared_ptr<AbstractScheduler>& instance);

  /**
   * The System runs without a Scheduler in most Tests and with one almost everywhere else. Tasks need to work
   * regardless of a Scheduler existing or not, use this method to query its existence.
   */
  static bool is_set();

  /**
   * If there is an active Scheduler, block execution until all @tasks have finished
   * If there is no active Scheduler, returns immediately since all @tasks have executed when they were scheduled
   */
  template <typename TaskType>
  static void wait_for_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks);

  template <typename TaskType>
  static void schedule_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks);

  template <typename TaskType>
  static void schedule_and_wait_for_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks);

 private:
  static std::shared_ptr<AbstractScheduler> _instance;
};

template <typename TaskType>
void CurrentScheduler::wait_for_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks) {
  if (IS_DEBUG) {
    for (auto& task : tasks) {
      if (!task->is_scheduled()) {
        Fail("Schedule tasks before joining them");
      }
    }
  }

  /**
   * In case wait_for_tasks() is called from a Task being executed in a Worker, block that worker, otherwise just
   * join the tasks
   */
  auto worker = Worker::get_this_thread_worker();
  if (worker) {
    worker->_wait_for_tasks(tasks);
  } else {
    for (auto& task : tasks) task->join();
  }
}

template <typename TaskType>
void CurrentScheduler::schedule_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks) {
  for (auto& task : tasks) {
    task->schedule();
  }
}

template <typename TaskType>
void CurrentScheduler::schedule_and_wait_for_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks) {
  schedule_tasks(tasks);
  wait_for_tasks(tasks);
}

}  // namespace opossum
