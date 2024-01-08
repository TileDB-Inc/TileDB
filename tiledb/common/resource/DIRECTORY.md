P* System multiprocessing primitives (mutex, semaphore)
# Resource Prediction and Measurement

This directory defines basic infrastructure for execution resources.

* Internal
    * Memory
    * Threads
    * Processor cores
    * System multiprocessing primitives (mutex, semaphore)
* External storage
    * Storage space within a Object store
    * Temporary file storage
* External network
    * Network bandwidth (bits per second)
    * Network usage (total bits)
  
## Large and Small Software

Most software is written as "small" software. The word "small" represents the relationship between the resource consumption of the software with respect to availability of resources from its computing environment. Small software assumes that resources are always available. Memory allocations always succeed. Disk storage never runs out of space. The network is always connected, support services never fail, and bandwidth is infinite. Everywhere there might be a resource limitation, it's ignored until it runs out, at which point the program terminates.

Small software is fine for small tasks. A mission-critical database system is not a small task. Resources on any particular compute node are always limited. There's never enough space for temporary files. Your process can run at low priority and with limited heap.

Large software requires paying attention to the resource issues that small software can ignore. It must make explicit such matters as budgets, estimates, allocations, and resource managers. It necessitates more verbose code, because things that could be defaulted, global, and implicit must be particular, local, and explicit. 

## Prediction and Measurement

Robustness is a system property, not a local responsibility. When you're inside a system that is dealing with failure recovery, it'll be obvious. Such a system will model tasks and jobs, progress and completion, resource dependencies, etc. All other components, those that feed into such a system, should abide by two principles of reliable software, summarized in brief:
* Don't mask failures.
* Fail as early as possible.

Masking including obvious things such as suppressing error entirely and converting to a generic error. It also include less clear tactics such retrying (at a local level) and falling back to alternative mechanism or resource (say, global heap instead of local heap). For resource limitations, avoiding masking means that if you don't obtain a resource, you fail.

Failing as early as possible requires more work. It's can be considered correct behavior to try something, get partway through, run out of resources, and halt. It's also inefficient if you can know in advance that it was going to run out. 

In a multitenant world, resources can run out by running out of budget in addition to running out of the primary resource. This leads to a hierarchy of failure:
* Exhaustion of primary resource. For example, in C++ this is `std::bad_alloc`. This is something approaching complete failure, just short of program termination. It should be avoided when at all possible, and yet when it does happen the system must cope with this state without crashing.
* Exhaustion of budgeted resource. This is when an operation is running but hits a budget limit. It might be possible to continue over-budget, but if every operation did this then we'd soon exhaust primary resources. In this case the operation must halt.
* Anticipated exhaustion. If you know how much resource allocation is required to do some operation and those resources are not available, it's better to not run the operation at all.

This brings us to three mechanisms needed for good resource management.
* Prediction. Prediction results in estimates that can be used to make resource decisions at the top level. In practice predictions are never perfect and should not be relied upon exclusively.
* Measurement. Measurement of budget usage creates knowledge of how much of a resource has been used by a particular operation.
* Exhaustion. We need to know when our primary resources are exhausted, rather than just seeing failures without knowing that this is the source.

Predictions are used to provide budget estimates. Measurement is used to enforce budget allocations. Exhaustion is the final backstop to forestall crashing.
