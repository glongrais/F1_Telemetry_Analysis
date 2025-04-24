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

class Agent:

    @classmethod
    def run(cls):
        file_list = DatabaseReader.get_team_radio_files()

        queue = Queue()
        db_writer_thread = Thread(target=Agent.__database_writer, args=(queue,))
        db_writer_thread.start()

        # Process files in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=5) as executor:
            list(tqdm(executor.map(lambda f: Agent.__transcribe(f, queue), file_list), total=len(file_list), desc="Processing files"))

        queue.join()
        queue.put(None)
        db_writer_thread.join()

    @classmethod
    def __transcribe(cls, filename, queue):
        try:
            wav_filename = filename[2].replace(".mp3", ".wav")
            sound = AudioSegment.from_mp3(filename[2])
            sound.export(wav_filename, format="wav")
            r = sr.Recognizer()
            with sr.AudioFile(wav_filename) as source:
                audio_data = r.record(source)
                init_options = {
                    "device": "auto",
                    "compute_type": "float32",
                    "download_root": "./models"
                }
                text = recognize(r, audio_data, model="small", language="en", init_options=init_options)
            os.remove(wav_filename)
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
