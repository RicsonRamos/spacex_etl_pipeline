select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    mission_id as unique_field,
    count(*) as n_records

from "spacex_db"."public"."stg_launches"
where mission_id is not null
group by mission_id
having count(*) > 1



      
    ) dbt_internal_test