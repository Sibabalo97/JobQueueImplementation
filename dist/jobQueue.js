// src/jobQueue.ts
/**
 * A queue for managing asynchronous jobs with concurrency, rate limiting, and timeout controls
 */
export class JobQueue {
    options;
    queue = [];
    activeCount = 0;
    isDisposed = false;
    jobHistory = [];
    rateLimitInterval = 60000; // 1 minute in ms
    /**
     * Creates a new JobQueue instance
     * @param options Configuration options for the queue
     */
    constructor(options = {}) {
        this.options = {
            concurrencyLimit: options.concurrencyLimit ?? 1000,
            rateLimit: options.rateLimit ?? Infinity,
            timeoutLimit: options.timeoutLimit ?? 1200
        };
    }
    /**
     * Schedule a job to be executed
     * @param fn The job function to execute
     * @param args Arguments to pass to the job function
     * @returns A promise that resolves with the job result
     * @throws Error if the queue has been disposed
     */
    schedule(fn, ...args) {
        if (this.isDisposed) {
            return Promise.reject(new Error('JobQueue has been disposed'));
        }
        return new Promise((resolve, reject) => {
            const job = {
                fn,
                args,
                resolve,
                reject,
                queuedAt: Date.now()
            };
            this.queue.push(job);
            this.processQueue();
        });
    }
    /**
     * Get the current size of the queue (excluding active jobs)
     * @returns The number of jobs waiting in the queue
     */
    size() {
        return this.queue.length;
    }
    /**
     * Get the number of currently active jobs
     * @returns The number of jobs currently being executed
     */
    active() {
        return this.activeCount;
    }
    /**
     * Dispose of the queue, rejecting all pending jobs
     */
    dispose() {
        this.isDisposed = true;
        // Reject all queued jobs
        while (this.queue.length > 0) {
            const job = this.queue.shift();
            if (job) {
                job.reject(new Error('JobQueue has been disposed'));
            }
        }
    }
    /**
     * Process the next job in the queue if conditions are met
     */
    async processQueue() {
        // If already at concurrency limit or no jobs, exit
        if (this.activeCount >= this.options.concurrencyLimit ||
            this.queue.length === 0 ||
            this.isDisposed) {
            return;
        }
        // Check rate limit
        if (this.options.rateLimit !== Infinity) {
            const now = Date.now();
            // Clean up history older than our rate limit interval
            this.jobHistory = this.jobHistory.filter(time => now - time < this.rateLimitInterval);
            if (this.jobHistory.length >= this.options.rateLimit) {
                const oldestJob = this.jobHistory[0];
                const waitTime = Math.max(0, this.rateLimitInterval - (now - oldestJob));
                if (waitTime > 0) {
                    await new Promise(resolve => setTimeout(resolve, waitTime));
                    // Re-process after waiting
                    if (!this.isDisposed) {
                        this.processQueue();
                    }
                    return;
                }
            }
        }
        // Take the next job from the queue
        const job = this.queue.shift();
        if (!job)
            return;
        this.activeCount++;
        // Record job start for rate limiting
        if (this.options.rateLimit !== Infinity) {
            this.jobHistory.push(Date.now());
        }
        const startTime = Date.now();
        const queueTime = startTime - job.queuedAt;
        // Create timeout controller if needed
        let timeoutId;
        let timeoutReject;
        const timeoutPromise = this.options.timeoutLimit !== Infinity
            ? new Promise((_, reject) => {
                timeoutReject = reject;
                timeoutId = setTimeout(() => {
                    reject(new Error(`Job timed out after ${this.options.timeoutLimit} seconds`));
                }, this.options.timeoutLimit * 1000);
            })
            : null;
        try {
            // Run the job with timeout if configured
            const result = timeoutPromise
                ? await Promise.race([job.fn(...job.args).catch(err => {
                        // Clear timeout if job fails
                        if (timeoutId)
                            clearTimeout(timeoutId);
                        throw err;
                    }), timeoutPromise])
                : await job.fn(...job.args);
            const endTime = Date.now();
            const executionTime = endTime - startTime;
            job.resolve({
                result,
                queueTime,
                executionTime
            });
        }
        catch (error) {
            // Clear timeout if it was set
            if (timeoutId) {
                clearTimeout(timeoutId);
            }
            job.reject(error);
        }
        finally {
            this.activeCount--;
            // Process next job in queue
            this.processQueue();
        }
    }
}
//# sourceMappingURL=jobQueue.js.map