from loguru import logger
from utils.logger import Logging

def log_sender(table_list, catalog_name, partition_date, message_producer):
    _id = Logging.get_function_name()
    logger.info(f"{_id}")
    table_iter = [df.iterrows() for df in table_list]
    
    for df_iter in table_iter:
        for _, doc in df_iter:
            log = eval(doc.to_json())
            log["DATA_SOURCE"] = catalog_name
            log["BATCH_DATE"] = partition_date
            try:
                message_producer.send_message(log)
            except Exception as e:
                logger.exception("EXCEPTION OCCURRED")
                print(e)
    logger.success(f"{_id} SUCCESS")