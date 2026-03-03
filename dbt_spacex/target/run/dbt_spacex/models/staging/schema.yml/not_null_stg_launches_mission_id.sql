select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select mission_id
from "spacex_db"."public"."stg_launches"
where mission_id is null



      
    ) dbt_internal_test