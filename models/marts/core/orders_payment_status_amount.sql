{% set payment_status = dbt_utils.get_column_values(ref('stg_payments'), "status") %}
select 
    order_id,
    {% for status in payment_status -%}
        coalesce(sum(case when status = '{{status}}' then amount end), 0) as {{status}}_amount
        {% if not loop.last%},{% endif %}
    {%- endfor%}
from {{ ref('stg_payments') }}
group by 1