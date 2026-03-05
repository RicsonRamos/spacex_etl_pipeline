select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select rocket_id as from_field
    from "spacex_db"."public"."stg_launches"
    where rocket_id is not null
),

parent as (
    select rocket_id as to_field
    from "spacex_db"."public"."stg_rockets"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test