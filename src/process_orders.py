from models import Order
from schemas import OrderSchema
from submit import SparkSubmit

if __name__ == "__main__":

    SparkSubmit().run(
        bucket_name='s3a://itau-shop',
        input_path='input/*.json',
        output_path='analytics',
        process_name='marketing_place',
        process=Order,
        schema=OrderSchema().get()
    )
