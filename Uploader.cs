using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NLog;

namespace Uploader
{
    /// <summary>
    /// SDK to upload files to a given backend.
    /// </summary>
    public sealed class Uploader : IDisposable
    {
        #region class fields and constructor

        /// <summary>
        /// Default number of Workers which are going to be simultaneously uploading files
        /// </summary>
        private const int DefaultNumberOfWorkers = 4;

        /// <summary>
        /// Logger instance to log error, debug and info messages
        /// </summary>
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        /// <summary>
        /// Lock for counting workers
        /// </summary>
        private readonly object workersCountLock = new object();

        /// <summary>
        /// Lock for enqueing data
        /// </summary>
        private readonly object enqueueLock = new object();

        /// <summary>
        /// Lock for initializing only once.
        /// </summary>
        private readonly object initializationLock = new object();

        /// <summary>
        /// Cancellation support.
        /// </summary>
        private readonly CancellationTokenSource cancelTokenSrc;

        /// <summary>
        /// Cancellation token
        /// </summary>
        private readonly CancellationToken cancellationToken;

        /// <summary>
        /// Task factory.
        /// </summary>
        private readonly TaskFactory taskFactory;

        /// <summary>
        /// Used to signal that all workers have been disposed
        /// </summary>
        private readonly ManualResetEvent workersDisposed = new ManualResetEvent(false);

        /// <summary>
        /// Blocking collection with pending files to be uploaded
        /// </summary>
        private readonly BlockingCollection<ScheduledUpload> pendingUploadCollection;

        /// <summary>
        /// Number of Workers which are going to be simultaneously uploading files
        /// </summary>
        private readonly int numberOfWorkers;

        /// <summary>
        /// Number of Workers which are currently created
        /// </summary>
        private int currentNumberOfWorkers = 0;

        /// <summary>
        /// This bool indicates whether this instance is already initialized or not
        /// </summary>
        private bool isInitialized = false;

        /// <summary>
        /// This bool indicates if tthis instance has been disposed and its resources have been shut down
        /// </summary>
        private bool isDisposed = false;

        /// <summary>
        /// True if this uploader is shutting down.  Pending operations should complete and uploaders should
        /// finish up.
        /// </summary>
        private volatile bool isShuttingDown = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="Uploader"/> class.
        /// </summary>
        /// <param name="numberOfWorkers">Number of threads that will upload files to the backend</param>
        public Uploader(int numberOfWorkers = DefaultNumberOfWorkers)
        {
            this.cancelTokenSrc = new CancellationTokenSource();
            this.cancellationToken = this.cancelTokenSrc.Token;
            this.taskFactory = new TaskFactory(this.cancellationToken);
            this.pendingUploadCollection = new BlockingCollection<ScheduledUpload>();
            this.numberOfWorkers = numberOfWorkers;
        }

        #endregion

        #region public API

        /// <summary>
        /// Callback for upload tasks. It returns a boolean indicating whether the upload was successful,
        /// the name of the uploaded file, and an error message with details on any errors when uploading.
        /// </summary>
        /// <param name="result">Boolean which indicates whether the upload was successful</param>
        /// <param name="file">Path to uploaded file</param>
        /// <param name="errorMessage">String with error details, if any</param>
        public delegate void UploadCallback(bool result, string file, string errorMessage);

        /// <summary>
        /// Initializes this instance. This method needs to be called exactly once before
        /// calling any of the other methods.
        /// </summary>
        /// <param name="backendUrl">Backend where the files will be uploaded.</param>
        /// <param name="authToken">Authorization token for backend.</param>
        /// <returns>True if it initializes correctly. False otherwise</returns>
        public bool Initialize(string backendUrl, string authToken)
        {
            lock (initializationLock)
            {
                if (this.isInitialized)
                {
                    Logger.Info("This instance is already initialized.");
                    return true;
                }

                if (string.IsNullOrWhiteSpace(authToken))
                {
                    Logger.Error("The parameter token cannot be null or empty");
                    return false;
                }

                // start background workers
                if (this.isShuttingDown)
                {
                    return false;
                }

                this.StartWorkers(backendUrl, authToken);
                this.isInitialized = true;

                return true;
            }
        }

        public bool PrepareForShutdown()
        {
            // Stop accepting new upload tasks.
            this.isShuttingDown = true;

            // Ask workers to complete and wait for them
            this.cancelTokenSrc.Cancel(false);
            int seconds = 0;
            while (this.workersDisposed.WaitOne(5000))
            {
                seconds = seconds + 5;
                Logger.Info("Waiting for uploader workers to complete ({0} seconds so far)", seconds);
            }

            return true;
        }

        /// <summary>
        /// Schedules a file to be uploaded to thebackend.
        /// </summary>
        /// <param name="filePath">Absolute path to the file that is going to be uploaded</param>
        /// <param name="callback">Optional callback which is sent back to the caller with information about the uploading outcome</param>
        /// <param name="deleteFile">Indicates if files need to be deleted after successful upload. Default is true</param>
        /// <returns>True if the file is scheduled for upload</returns>
        public bool UploadFileFile(string filePath, UploadCallback callback = null, bool deleteFile = true)
        {
            lock (enqueueLock)
            {
                if (this.isDisposed || this.isShuttingDown)
                {
                    Logger.Error("This instance is disposed or shutting down.");
                    return false;
                }
                if (!this.isInitialized)
                {
                    Logger.Error("This instance is not initialized. Call Initialize() first");
                    return false;
                }
                var fileFullPath = Path.GetFullPath(filePath);

                Logger.Debug("Scheduling upload of file {0}", fileFullPath);

                // set up file makes all the initial validations
                var fileToUpload = new ScheduledUpload(fileFullPath)
                {
                    DeleteFile = deleteFile,
                    Callback = callback
                };

                // now enqueue the file for upload
                if (this.pendingUploadCollection.TryAdd(fileToUpload))
                {
                    Logger.Info("The dialogue file {0} is scheduled for upload", fileFullPath);
                    return true;
                }
                else
                {
                    // blocking collection is full
                    Logger.Error("Cannot upload dialogue file {0} since upload queue is full", fileFullPath);
                    return false;
                }
            }
        }

        /// <summary>
        /// Returns the current number of enqueued jobs to be uploaded.
        /// </summary>
        /// <returns>current number of enqueued jobs</returns>
        public int EnqueuedJobs()
        {
            return this.pendingUploadCollection.Count;
        }

        /// <summary>
        /// Dispose this instance and all resources. Threads uploading files are stopped
        /// and the upload resumes when restarted.
        /// </summary>
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            this.Dispose(true);

            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }

        #endregion

        #region blocking queue

        /// <summary>
        /// This method creates several worker threads which are in charge to upload files to the backend
        /// </summary>
        /// <param name="backendUrl">Backend where the files will be uploaded.</param>
        /// <param name="authToken">Authorization token for backend.</param>
        /// <returns>True if it initializes correctly. False otherwise</returns>
        private void StartWorkers(string backendUrl, string authToken)
        {
            this.workersDisposed.Reset();
            for (int i = 0; i < this.numberOfWorkers; i++)
            {
                int threadId = i;
                this.taskFactory.StartNew(
                    () =>
                    {
                        Logger.Debug("Starting worker {0}", threadId);
                        lock (workersCountLock)
                        {
                            currentNumberOfWorkers++;
                            Logger.Debug("Current number of workers: {0}", currentNumberOfWorkers);
                        }

                        // each worker has its own instance of RestHelper
                        var restHelper = new RestHelper(backendUrl, authToken);

                        while (!this.cancellationToken.IsCancellationRequested)
                        {
                            try
                            {
                                // try to take a file from the queue. If there is no file it blocks here
                                var pendingFile = this.pendingUploadCollection.Take(this.cancellationToken);
                                if (pendingFile != null)
                                {
                                    Logger.Debug("Worker {0}: will upload {1}", threadId, pendingFile.FilePath);

                                    // we do a synchronous upload here, since the blocking queue took
                                    // care of implementing asynchronous behavior
                                    var result = restHelper.UploadFile(pendingFile, backendUrl);
                                    string callbackStr = result.Success ?
                                            string.Format("File {0} was uploaded correctly.", pendingFile.FilePath) :
                                            result.ErrorMessage;

                                    if (result.Success && pendingFile.DeleteFile)
                                    {
                                        this.DeletePendingFile(pendingFile);
                                    }

                                    // callback can be null
                                    pendingFile.Callback?.Invoke(result.Success, pendingFile.FilePath, callbackStr);
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                // Operation was cancelled while blocked on Take operation
                                // It should be safe to drop out of the loop here
                                break;
                            }
                            catch (Exception e)
                            {
                                // Something went wrong and we should note it in the log and then try to recover
                                Logger.Error(e, "Worker {0}: Ignoring exception {1}", threadId, e.Message);
                            }
                        }

                        // Worker finished, cleanup
                        Logger.Debug("Finishing worker {0}", threadId);
                        this.CleanUpAfterWorkerFinished();

                    }, this.cancellationToken);
            }
        }

        private void CleanUpAfterWorkerFinished()
        {
            lock (workersCountLock)
            {
                currentNumberOfWorkers--;
                Logger.Debug("Current number of workers: {0}", currentNumberOfWorkers);
                if (currentNumberOfWorkers <= 0)
                {
                    workersDisposed.Set();
                }
            }
        }

        #endregion

        #region pending files

        /// <summary>
        /// Deletes a file after successful upload from the DB, and from disk if this was instructed by the caller.
        /// </summary>
        /// <param name="fileToDelete">The pending file to be deleted</param>
        private void DeletePendingFile(ScheduledUpload fileToDelete)
        {
            if (fileToDelete.DeleteFile)
            {
                if (File.Exists(fileToDelete.FilePath))
                {
                    try
                    {
                        File.Delete(fileToDelete.FilePath);
                    }
                    catch (IOException e)
                    {
                        Logger.Error("Cannot delete uploaded file {0}: {1}", fileToDelete.FilePath, e.Message);
                    }
                }
            }
        }
        #endregion

        #region IDisposable Support

        private void Dispose(bool disposing)
        {
            if (!this.isDisposed)
            {

                // Ask all existing processes to quit unless they are already complete
                // If PrepareForShutdown was called there should be no active workers
                if (this.numberOfWorkers > 0)
                {
                    Logger.Debug("Cancelling worker tasks.");
                    this.cancelTokenSrc.Cancel(false);

                    // now we wait until all workers are disposed
                    // But we wait no more than 10 seconds in case of a race condition with PrepareForShutdown
                    workersDisposed.WaitOne(10000);
                }
                if (disposing)
                {
                    // Dispose managed state (managed objects).
                    Logger.Debug("Disposing worker queue");
                    this.cancelTokenSrc.Dispose();
                    this.pendingUploadCollection.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.
                this.isDisposed = true;
            }
        }

        #endregion

    }
}

