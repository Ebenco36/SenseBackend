import sys
import os
import traceback
sys.path.append(os.getcwd())
from concurrent.futures import ThreadPoolExecutor
from src.Services.DatabaseHandler import DatabaseHandler
from src.Commands.PaperProcessor import PaperProcessor
from src.Commands.DatabaseUpdater import DatabaseUpdater


class PaperProcessorPipeline:
    def __init__(self, table_name, column_mapping, tracker_table='processing_tracker_v2'):
        self.table_name = table_name
        self.column_mapping = column_mapping
        self.tracker_table = tracker_table

    def ensure_tracker_table_exists(self, db_handler):
        """
        Creates tracker table if not exists, includes processed_ids, failed_ids, and last_processed_id.
        """
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.tracker_table} (
            source_name VARCHAR(255) PRIMARY KEY,
            processed_ids TEXT DEFAULT '',
            failed_ids TEXT DEFAULT '',
            last_processed_id BIGINT DEFAULT 0,
            status VARCHAR(50) DEFAULT 'pending',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        db_handler.execute_query(create_table_query)

    def get_tracker_info(self, db_handler, source_name):
        """
        Returns processed_ids and failed_ids as sets, plus last_processed_id for a source.
        """
        query = f"""
        SELECT processed_ids, failed_ids, last_processed_id
        FROM {self.tracker_table} WHERE source_name = %s
        """
        result = db_handler.execute_query(query, (source_name,))
        if result:
            processed_raw, failed_raw, last_id = result[0]
            processed_set = set(map(int, processed_raw.split(','))) if processed_raw else set()
            failed_set = set(map(int, failed_raw.split(','))) if failed_raw else set()
            return processed_set, failed_set, last_id or 0
        return set(), set(), 0

    def update_tracker(self, db_handler, source_name, new_processed_ids, retry_success_ids, new_failed_ids, last_id, status='in_progress'):
        """
        Updates processed_ids, failed_ids, and last_processed_id in the tracker table.
        """
        processed_ids, failed_ids, _ = self.get_tracker_info(db_handler, source_name)

        processed_ids.update(new_processed_ids)
        failed_ids.difference_update(retry_success_ids)
        failed_ids.update(new_failed_ids)

        processed_str = ",".join(map(str, sorted(processed_ids)))
        failed_str = ",".join(map(str, sorted(failed_ids)))

        query = f"""
        INSERT INTO {self.tracker_table} 
            (source_name, processed_ids, failed_ids, last_processed_id, status, updated_at)
        VALUES 
            (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (source_name)
        DO UPDATE SET
            processed_ids = EXCLUDED.processed_ids,
            failed_ids = EXCLUDED.failed_ids,
            last_processed_id = EXCLUDED.last_processed_id,
            status = EXCLUDED.status,
            updated_at = CURRENT_TIMESTAMP
        """
        db_handler.execute_query(query, (source_name, processed_str, failed_str, last_id, status))

    def process_source_in_batches(self, query, csv_file_path, db_name, batch_size=100):
        """
        Dynamically processes all missing primary_ids (including skipped ones).
        """
        db_handler = DatabaseHandler(query)
        updater = DatabaseUpdater(table_name=self.table_name, column_mapping=self.column_mapping)

        try:
            self.ensure_tracker_table_exists(db_handler)

            # 1. Get processed & failed IDs
            processed_ids, failed_ids, _ = self.get_tracker_info(db_handler, db_name)

            # 2. Fetch all candidate primary_ids
            fetch_ids_query = f"SELECT primary_id FROM ({query}) AS subq"
            all_ids_result = db_handler.execute_query(fetch_ids_query)
            all_ids = [row[0] for row in all_ids_result] if all_ids_result else []

            # 3. Filter out already processed
            to_process_ids = sorted(set(all_ids) - processed_ids)

            for i in range(0, len(to_process_ids), batch_size):
                batch_ids = to_process_ids[i:i + batch_size]
                placeholders = ",".join(map(str, batch_ids))
                batch_query = f"{query} AND primary_id IN ({placeholders})"

                db_handler.query = batch_query
                processor = PaperProcessor(db_handler=db_handler, csv_file_path=csv_file_path)
                data_return = processor.process_papers(db_name=db_name)
                # print(data_return)
                if data_return.empty:
                    continue

                # Mark retry rows
                data_return["is_retry"] = data_return["id"].isin(failed_ids)
                retry_ids = set(data_return[data_return["is_retry"]]["id"])
                new_ids = set(data_return[~data_return["is_retry"]]["id"])

                retry_success = set()
                failed_this_batch = set()

                try:
                    updater.update_columns_for_existing_records(data_return.drop(columns="is_retry"), id_column='id')
                    retry_success = retry_ids
                except Exception as err:
                    print(f"Partial failure during update: {err}")
                    failed_this_batch = retry_ids.union(new_ids)

                # Always update with max ID to keep progress tracking
                last_id = max(batch_ids)

                self.update_tracker(
                    db_handler,
                    db_name,
                    new_processed_ids=new_ids.union(retry_success),
                    retry_success_ids=retry_success,
                    new_failed_ids=failed_this_batch,
                    last_id=last_id,
                    status='in_progress'
                )

            # All done
            self.update_tracker(db_handler, db_name, set(), set(), set(), max(all_ids, default=0), status='completed')

        except Exception as e:
            traceback.print_exc()
            print(f"Error processing source {db_name}: {e}")
        finally:
            # db_handler.close_connection()
            updater.close_connection()


# def main():
#     pipeline = PaperProcessorPipeline(
#         table_name='all_db',
#         column_mapping={'Id': 'primary_id'}
#     )

#     sources = [
#         {
#             "query": "SELECT primary_id, \"DOI\" FROM all_db WHERE \"Source\"='Cochrane'",
#             "csv_file_path": "Data/output/papers_data",
#             "db_name": "Cochrane"
#         },
#         {
#             "query": "SELECT primary_id, \"DOI\" FROM all_db WHERE \"Source\"='LOVE'",
#             "csv_file_path": "Data/output/papers_data_love",
#             "db_name": "LOVE"
#         },
#         {
#             "query": "SELECT primary_id, \"DOI\" FROM all_db WHERE \"Source\"='OVID'",
#             "csv_file_path": "Data/output/papers_data_OVID",
#             "db_name": "OVID"
#         },
#         {
#             "query": "SELECT primary_id, \"DOI\" FROM all_db WHERE \"Source\"='Medline'",
#             "csv_file_path": "Data/output/papers_data_medline",
#             "db_name": "Medline"
#         }
#     ]

#     # Use parallel processing to process multiple sources simultaneously
#     with ThreadPoolExecutor(max_workers=4) as executor:
#         executor.map(
#             lambda source: pipeline.process_source_in_batches(
#                 query=source["query"],
#                 csv_file_path=source["csv_file_path"],
#                 db_name=source["db_name"],
#                 batch_size=100
#             ),
#             sources
#         )

# if __name__ == "__main__":
#     main()