from tqdm import tqdm
from queue import Queue
from threading import Thread
import db_writer
import db_reader
import mlx_whisper


class Agent:

    @classmethod
    def run(cls):
        file_list = db_reader.get_team_radio_files()

        import ssl
        ssl._create_default_https_context = ssl._create_unverified_context

        conn = db_writer.get_connection()
        queue = Queue()
        db_writer_thread = Thread(target=Agent.__database_writer, args=(queue, conn))
        db_writer_thread.start()

        for file in tqdm(file_list, desc="Transcribing team radios"):
            Agent.__transcribe(file, queue)

        queue.join()
        queue.put(None)
        db_writer_thread.join()
        conn.close()

    @classmethod
    def __transcribe(cls, file, queue):
        path = file['team_radio_file']
        try:
            text = mlx_whisper.transcribe(
                path,
                path_or_hf_repo="mlx-community/whisper-large-v3-turbo"
            )["text"]
            queue.put((file['team_radio_file_id'], file['team_radio_id'], text))
        except Exception as e:
            print(f"Error processing {path}: {e}")

    @classmethod
    def __database_writer(cls, queue, conn):
        while True:
            item = queue.get()
            if item is None:
                break
            file_id, radio_id, text = item
            try:
                db_writer.insert_team_radio_text(conn, file_id, radio_id, text)
            except Exception as e:
                print(f"Error writing {file_id} to database: {e}")
            finally:
                queue.task_done()
