import speech_recognition as sr
from speech_recognition.recognizers.whisper_local.faster_whisper import recognize, InitOptionalParameters
from pydub import AudioSegment
import os
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from queue import Queue
from threading import Thread
from db_writer import DatabaseWriter
from db_reader import DatabaseReader
import mlx_whisper

class Agent:

    @classmethod
    def run(cls):
        file_list = DatabaseReader.get_team_radio_files()

        import ssl
        ssl._create_default_https_context = ssl._create_unverified_context

        #model = whisper.load_model("turbo", download_root= "./models", in_memory=True)
        queue = Queue()
        db_writer_thread = Thread(target=Agent.__database_writer, args=(queue,))
        db_writer_thread.start()

        for file in tqdm(file_list, total=len(file_list), desc="Processing files"):
            Agent.__transcribe(file, queue)

        queue.join()
        queue.put(None)
        db_writer_thread.join()

    @classmethod
    def __transcribe(cls, filename, queue):
        try:
            #wav_filename = filename[2].replace(".mp3", ".wav")
            #sound = AudioSegment.from_mp3(filename[2])
            #sound.export(wav_filename, format="wav")
            #r = sr.Recognizer()
            #with sr.AudioFile(wav_filename) as source:
            #    audio_data = r.record(source)
            #    init_options = {
            #        "device": "auto",
            #        "compute_type": "float32",
            #        "download_root": "./models"
            #    }
            #    text = recognize(r, audio_data, model="medium", language="en", init_options=init_options)
            #os.remove(wav_filename)
            text = mlx_whisper.transcribe(filename[2], path_or_hf_repo="mlx-community/whisper-large-v3-turbo")["text"]
            queue.put((filename[0], filename[1], text))
        except Exception as e:
            print(f"Error processing {filename[2]}: {e}")

    @classmethod
    def __database_writer(cls, queue):
        """Thread function to write transcriptions to the database."""
        while True:
            item = queue.get()
            if item is None:  # Sentinel value to stop the thread
                break
            file_id, radio_id, text = item
            try:
                DatabaseWriter.insert_team_radio_text(file_id, radio_id, text)
            except Exception as e:
                print(f"Error writing {file_id} to database: {e}")
            finally:
                queue.task_done()
