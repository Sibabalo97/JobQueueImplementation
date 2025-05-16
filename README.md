JobQueue
A flexible TypeScript-based job queue with concurrency control, rate limiting, and timeout functionality.
Features

Concurrency Limiting: Control how many jobs can run in parallel
Rate Limiting: Limit number of jobs executed per minute
Timeout Control: Automatically fail jobs that take too long
Queue Monitoring: Track queue size and active job count
FIFO Ordering: Jobs are executed in the order they were scheduled
Resource Management: Proper cleanup with dispose functionality

Getting Started
Prerequisites

Node.js 18.x or higher
npm 7.x or higher

Installation

Clone the repository:

bash git clone https://github.com/Sibabalo97/JobQueueImplementation.git

cd to obQueueImplementation

![image](https://github.com/user-attachments/assets/5efc1b1d-c34b-4e81-be50-a36925d89969)


Install dependencies:

bash

npm install

Run the tests:

bash

npm test

or  

npm run test


Usage Example
typescriptimport { JobQueue } from './src/jobQueue.js';

// Create a queue with options
const queue = new JobQueue({
  concurrencyLimit: 3,  // Run up to 3 jobs at once
  rateLimit: 10,        // Process max 10 jobs per minute
  timeoutLimit: 5       // Fail jobs that take longer than 5 seconds
});

// Define a job function
async function processItem(id, data) {
  console.log(`Processing item ${id}...`);
  // Simulate work
  await new Promise(resolve => setTimeout(resolve, 1000));
  return `Processed ${id}: ${data}`;
}

// Schedule jobs
async function main() {
  try {
    const results = await Promise.all([
      queue.schedule(processItem, 1, "data-1"),
      queue.schedule(processItem, 2, "data-2"),
      queue.schedule(processItem, 3, "data-3"),
      queue.schedule(processItem, 4, "data-4"),
      queue.schedule(processItem, 5, "data-5")
    ]);
    
    results.forEach(result => {
      console.log(`Result: ${result.result}`);
      console.log(`Time in queue: ${result.queueTime}ms`);
      console.log(`Execution time: ${result.executionTime}ms`);
    });
  } catch (error) {
    console.error('Job failed:', error);
  } finally {
    // Always dispose the queue when done
    queue.dispose();
  }
}

main().catch(console.error);
API Reference
JobQueue Class
Constructor Options
typescriptinterface JobQueueOptions {
  concurrencyLimit?: number;  // Default: Infinity
  rateLimit?: number;         // Default: Infinity (jobs per minute)
  timeoutLimit?: number;      // Default: Infinity (seconds)
}
Methods

schedule<T>(fn: (...args: any[]) => Promise<T>, ...args: any[]): Promise<JobResult<T>>
Schedule a job to be executed with the given arguments.
size(): number
Returns the number of jobs currently waiting in the queue.
active(): number
Returns the number of jobs currently being executed.
dispose(): void
Rejects all pending jobs and cleans up resources.

Job Results

typescriptinterface JobResult<T> {
  result: T;           // The value returned by the job function
  queueTime: number;   // Time spent waiting in the queue (ms)
  executionTime: number; // Time spent executing the job (ms)
}
Development

Project Structure
job-queue/
├── src/
│   ├── jobQueue.ts     # Main implementation
│   ├── index.ts        # Main export file
│   └── tests/
│       └── jobQueue.test.ts  # Test suite
├── package.json
└── tsconfig.json

![image](https://github.com/user-attachments/assets/7e03a0a4-4d94-466c-90aa-cc62a0044b05)


Running Tests
bashnpm test
Building for Production
bashnpm run build
This will compile TypeScript files to JavaScript in the dist directory.

tests in the Terminal 
![image](https://github.com/user-attachments/assets/10d0110f-88d5-40d8-9996-723e9f54c86d)

![image](https://github.com/user-attachments/assets/7ab7b749-a330-4688-8196-6d2c3edc6652)

![image](https://github.com/user-attachments/assets/d2102935-3165-4849-aef6-18d101085cdf)

![image](https://github.com/user-attachments/assets/9f80de74-962f-44e4-92e3-40bb6e1e7688)







