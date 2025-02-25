{{config(materialized = 'table', schema = 'transforming_dev')}}
 
with recursive managers
     
      (indent, office , employee_id, manager_id, employee_title, manager_title)
    as
      (
 
        -- Anchor Clause
        select '*' as indent, office, empid, CAST(IFF(reportsto is null, empid, reportsto) as number) as manager_id,
        title as employee_title, title as manager_title from {{ref('stg_employee')}}   
          where title = 'President'
 
        union all
 
        -- Recursive Clause
        select
            indent || ' *' as indent, emp.office, emp.empid, emp.reportsto, emp.title,
            mgr.employee_title
          from {{ref('stg_employee')}} as emp inner join managers as mgr
            on emp.reportsto = mgr.employee_id
      ),
 
      offices (office, officecity, officecountry)
      as
      (
      select office, officecity, officecountry from {{ref('stg_offices')}}
      )
 
  -- This is the "main select".
  select managers.indent, managers.employee_id, offices.officecity, offices.officecountry,
  managers.employee_title, managers.manager_id, managers.manager_title
    from managers left join offices on managers.office = offices.office