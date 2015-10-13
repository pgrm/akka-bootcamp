using System;
using System.IO;
using System.Text;
using Akka.Actor;

namespace WinTail
{
    public class TailActor : UntypedActor
    {
        private const int MaxCharsToRead = 1024 * 1024; // 1MB
        private readonly string _filePath;
        private readonly IActorRef _reporterActor;
        private FileObserver _observer;
        private StreamReader _fileStreamReader;

        public TailActor(IActorRef reporterActor, string filePath)
        {
            _reporterActor = reporterActor;
            _filePath = filePath;
        }

        protected override void PreStart()
        {
            // start watching file for changes
            _observer = new FileObserver(Self, Path.GetFullPath(_filePath));
            _observer.Start();

            // open the file stream with shared read/write permissions (so file can be written to while open)
            var fileStream = new FileStream(Path.GetFullPath(_filePath), FileMode.Open, FileAccess.ReadWrite);
            _fileStreamReader = new StreamReader(fileStream, Encoding.UTF8);

            // the file probably has changed so start reading it
            Self.Tell(new FileWrite(_filePath));
            base.PreStart();
        }

        protected override void OnReceive(object message)
        {
            if (message is FileWrite)
            {
                // move file cursor forward
                // pull results from cursor to end of file and write to output
                // (tis is assuming a log file type format that is append-only)
                var text = ReadToEnd();
                if (!string.IsNullOrEmpty(text))
                {
                    _reporterActor.Tell(text);
                }
            }
            else if (message is FileError)
            {
                var fe = message as FileError;
                _reporterActor.Tell($"Tail error: {fe.Reason}");
            }
        }

        protected override void PostStop()
        {
            _observer.Dispose();
            _observer = null;
            _fileStreamReader.Close();
            _fileStreamReader = null;
            base.PostStop();
        }

        private string ReadToEnd() 
        {
            var buffer = new char[MaxCharsToRead];
            var charsRead = _fileStreamReader.ReadBlock(buffer, 0, MaxCharsToRead);

            if (charsRead == buffer.Length)
            {
                Self.Tell(new FileWrite(_filePath));   
            }
            return new string(buffer);
        }

        #region Message types

        /// <summary>
        /// Signal that the file has changed, and we need to read the next line of the file.
        /// </summary>
        public class FileWrite
        {
            public FileWrite(string fileName)
            {
                FileName = fileName;
            }

            public string FileName { get; private set; }
        }

        /// <summary>
        /// Signal that the OS had an error accessing the file.
        /// </summary>
        public class FileError
        {
            public FileError(string fileName, string reason)
            {
                FileName = fileName;
                Reason = reason;
            }

            public string FileName { get; private set; }

            public string Reason { get; private set; }
        }

        /// <summary>
        /// Signal to read the initial contents of the file at actor startup.
        /// </summary>
        public class InitialRead
        {
            public InitialRead(string fileName, string text)
            {
                FileName = fileName;
                Text = text;
            }

            public string FileName { get; private set; }
            public string Text { get; private set; }
        }

        #endregion
    }
}

