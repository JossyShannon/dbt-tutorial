select
    customer_id
from {{ ref('stg_customer') }}
group by customer_id
having count(*) > 1