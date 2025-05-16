/**
 * Options for configuring the JobQueue
 */
export interface JobQueueOptions {
    /**
     * Maximum number of jobs that can run concurrently
     * @default 1000
     */
    concurrencyLimit?: number;
    /**
     * Rate limit in jobs per minute
     * @default Infinity
     */
    rateLimit?: number;
    /**
     * Job timeout in seconds
     * @default 1200
     */
    timeoutLimit?: number;
}
/**
 * Result of a job execution
 */
export interface JobResult<T> {
    /**
     * The result of the job execution
     */
    result: T;
    /**
     * Time spent in queue before execution (milliseconds)
     */
    queueTime: number;
    /**
     * Time spent executing the job (milliseconds)
     */
    executionTime: number;
}
/**
 * A queue for managing asynchronous jobs with concurrency, rate limiting, and timeout controls
 */
export declare class JobQueue {
    private options;
    private queue;
    private activeCount;
    private isDisposed;
    private jobHistory;
    private rateLimitInterval;
    /**
     * Creates a new JobQueue instance
     * @param options Configuration options for the queue
     */
    constructor(options?: JobQueueOptions);
    /**
     * Schedule a job to be executed
     * @param fn The job function to execute
     * @param args Arguments to pass to the job function
     * @returns A promise that resolves with the job result
     * @throws Error if the queue has been disposed
     */
    schedule<T>(fn: (...args: any[]) => Promise<T>, ...args: any[]): Promise<JobResult<T>>;
    /**
     * Get the current size of the queue (excluding active jobs)
     * @returns The number of jobs waiting in the queue
     */
    size(): number;
    /**
     * Get the number of currently active jobs
     * @returns The number of jobs currently being executed
     */
    active(): number;
    /**
     * Dispose of the queue, rejecting all pending jobs
     */
    dispose(): void;
    /**
     * Process the next job in the queue if conditions are met
     */
    private processQueue;
}
