using System;
using System.IO;
using System.Net.Http;
using NLog;
using RestSharp;

namespace Uploader
{
    /// <summary>
    /// This class implements a REST client that talks to a REST API
    /// </summary>
    internal sealed class RestHelper
    {
        /// <summary>
        /// Auth-Token in metadata
        /// </summary>
        private const string AuthTokenKey = "Auth-Token";

        /// <summary>
        /// Content-Type in metadata
        /// </summary>
        private const string ContentTypeKey = "Content-Type";

        /// <summary>
        /// Json content type
        /// </summary>
        private const string JsonContentType = "application/json";

        /// <summary>
        /// Logger instance to log error, debug and info messages
        /// </summary>
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        /// <summary>
        /// Authorization token for LDS Backend.
        /// </summary>
        private readonly string authToken;

        /// <summary>
        /// Backend url
        /// </summary>
        private readonly string backendUrl;

        /// <summary>
        /// Initializes a new instance of the <see cref="RestHelper"/> class.
        /// </summary>
        /// <param name="backendUrl">Backend where the files will be uploaded.</param>
        /// <param name="authToken">Authorization token for backend.</param>
        internal RestHelper(string backendUrl, string authToken)
        {
            this.authToken = authToken;
            this.backendUrl = backendUrl;
        }

        #region private helper classes

        /// <summary>
        /// Used to return better error messages in callbacks when something goes wrong.
        /// </summary>
        internal class UploadResult
        {
            /// <summary>
            /// True if the upload operation was successful
            /// </summary>
            internal bool Success { get; }

            /// <summary>
            /// Gets error message, in case Success is false. Otherwise it might be empty.
            /// </summary>
            internal string ErrorMessage { get; }

            /// <summary>
            /// Initializes a new instance of the <see cref="UploadResult"/> class.
            /// </summary>
            /// <param name="success">Upload was succesful</param>
            /// <param name="error">Error message, if any</param>
            internal UploadResult(bool success, string error)
            {
                this.Success = success;
                this.ErrorMessage = error;
            }
        }

        #endregion

        #region upload


        /// <summary>
        /// Uploads a file to the backend.
        /// </summary>
        /// <param name="fileToUpload">A file to be uploaded to the backend.</param>
        /// <param name="putUrl">url where file is to be uploaded</param>
        /// <returns>A <see cref="UploadResult"/> indicating the outcome of the upload process</returns>
        internal UploadResult UploadFile(ScheduledUpload fileToUpload, string putUrl)
        {
            if (string.IsNullOrWhiteSpace(putUrl))
            {
                Logger.Error("No resource Id for file {0}", fileToUpload.FilePath);
                return new UploadResult(false, string.Format("No resource Id for file {0}", fileToUpload.FilePath));
            }

            const string fileContentType = "application/x-www-form-urlencoded";
            byte[] fileInBytes = File.ReadAllBytes(fileToUpload.FilePath);

            var restRequestFile = new RestRequest(putUrl, Method.PUT)
            {
                RequestFormat = DataFormat.Xml
            };
            restRequestFile.AddHeader(AuthTokenKey, this.authToken);
            restRequestFile.AddHeader(ContentTypeKey, fileContentType);
            restRequestFile.AddParameter(fileContentType, fileInBytes, fileContentType, ParameterType.RequestBody);

            var restClient = new RestClient(new Uri(this.backendUrl));
            var response = restClient.Put(restRequestFile);

            switch (response.StatusCode)
            {
                case System.Net.HttpStatusCode.Unauthorized:
                    return new UploadResult(false, string.Format("Unauthorized to upload {0}", fileToUpload.FilePath));

                case System.Net.HttpStatusCode.OK:
                    // file has been uploaded, thus we can remove it from pending files list
                    Logger.Info("{0} was uploaded correctly: {1}", fileToUpload.FilePath, response.StatusDescription);
                    return new UploadResult(true, string.Empty);

                default:
                    Logger.Error("Could not upload {0}: {1} - {2}", fileToUpload.FilePath, Convert.ToInt32(response.StatusCode), response.Content);
                    return new UploadResult(false, string.Format("Could not upload {0}: {1} - {2}", fileToUpload.FilePath, Convert.ToInt32(response.StatusCode), response.StatusDescription));
            }
        }

        #endregion

    }
}
