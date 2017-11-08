using System;
using System.IO;

namespace Uploader
{
    /// <summary>
    /// Represents a file that is scheduled for upload to the Backend.
    /// </summary>
    internal sealed class ScheduledUpload
    {
        /// <summary>
        /// Gets or sets the path to the file
        /// </summary>
        public string FilePath { get; private set; }

        /// <summary>
        /// Gets or sets a value indicating whether this file is to be deleted after upload
        /// </summary>
        public bool DeleteFile { get; set; }

        /// <summary>
        /// Callback from caller to notify on upload results
        /// </summary>
        public Uploader.UploadCallback Callback { get; set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="filePath">path to the file that needs to be uploaded</param>
        public ScheduledUpload(string filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath))
            {
                throw new ArgumentException("Argument path cannot be empty");
            }

            this.FilePath = Path.GetFullPath(filePath);
        }
    }
}
