using System;
using System.IO;
using Akka.Actor;

namespace WinTail
{
    /// <summary>
    /// Actor that validates user input and signals result to others.
    /// </summary>
    public class FileValidatorActor : UntypedActor
    {
        private const string TailCoordinatorActorPath = "akka://MyActorSystem/user/tailCoordinatorActor";
        private readonly IActorRef _consoleWriterActor;

        public FileValidatorActor(IActorRef consoleWriterActor)
        {
            _consoleWriterActor = consoleWriterActor;
        }

        protected override void OnReceive(object message)
        {
            var msg = message as string;
            if (string.IsNullOrEmpty(msg))
            {
                // signal that the user needs to supply an input
                _consoleWriterActor.Tell(new Messages.NullInputError("No input received."));
                Sender.Tell(new Messages.ContinueProcessing());
            }
            else
            {
                var valid = IsFileUri(msg);
                if (valid)
                {
                    // send success to console writer
                    _consoleWriterActor.Tell(new Messages.InputSuccess($"Starting processing for {msg}"));
                    Context.ActorSelection(TailCoordinatorActorPath).Tell(new TailCoordinatorActor.StartTail(msg, _consoleWriterActor));
                }
                else
                {
                    // signal that input was bad
                    _consoleWriterActor.Tell(new Messages.ValidationError($"Invalid: input '{msg}' is not a file."));
                    Sender.Tell(new Messages.ContinueProcessing());
                }
            }
        }

        private static bool IsFileUri(string path) 
        {
            return File.Exists(path);
        }
    }
}

