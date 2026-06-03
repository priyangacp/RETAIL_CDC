{{
    config(
        materialized='incremental',
        unique_key=['ORDER_ID', 'SHIPMENT_ID'],
        on_schema_change='fail',

        pre_hook="{{ replay_from_archive(
            queue_table_name='orders_queue',
            hook_type='pre'
        ) }}",

        post_hook=[
            "{{ cdc_merge(
                source('bronze', 'orders_queue'),
                this,
                ['ORDER_ID', 'SHIPMENT_ID']
            ) }}",
            "{{ replay_from_archive(
                queue_table_name='orders_queue',
                hook_type='post'
            ) }}"
        ]
    )
}}


select * from {{ source('bronze', 'orders_queue') }} where 1 = 0