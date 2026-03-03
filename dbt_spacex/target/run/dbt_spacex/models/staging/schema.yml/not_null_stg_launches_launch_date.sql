select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select launch_date
from "spacex_db"."public"."stg_launches"
where launch_date is null



      
    ) dbt_internal_test