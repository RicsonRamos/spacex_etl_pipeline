
    
    

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


