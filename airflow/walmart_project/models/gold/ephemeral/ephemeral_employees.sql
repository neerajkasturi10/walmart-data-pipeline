select
    employee_id,
    store_id as store_id,
    emp_f_name as employee_first_name,
    emp_l_name as employee_last_name,
    concat(emp_f_name,' ',emp_l_name) as employee_full_name,
    emp_email as employee_email,
    emp_job_title as employee_job_title,
    emp_salary as employee_salary,
    case 
        when emp_salary >= 60000 then 'High' 
        when emp_salary >= 40000 and emp_salary < 60000 then 'Medium' 
        when emp_salary >= 20000 and emp_salary < 40000 then 'Low' 
        else 'Unknown' 
    end as employee_salary_category,
    emp_is_active as employee_is_active,
    emp_created_ts as employee_created_timestamp,
    emp_updated_ts as employee_updated_timestamp
from {{ref('obt_b')}}