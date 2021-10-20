const {fetchWhereCondition} = require('../../utils/common.util');

const QUERY_TREND_BY_DATE = async (params) => {
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  const sqlForGroupBy = params.query.fetchBy === 'HOURS' ? ' group by date , hours  order by date , hours ' : ' group by date order by date';
  const filterQuery = params.query.fetchBy === 'HOURS' ? ' start_time :: date as date ,  hour( start_time ) as hours ' : ' start_time :: date as date';
  return `select   ${filterQuery}  , count(query_id) as query_count   
  from snowflake.account_usage.query_history  
  where start_time between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') and warehouse_size <> '' and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} )
  ${sqlForGroupBy}`
;
}

const QUERY_TREND_BY_WEEKDAY = async (params) => { 
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return `
  select  dayname( start_time ) as day    , dayofweek( start_time ) day_of_week , 
  count(query_id) as query_count   
  from snowflake.account_usage.query_history  
  where start_time between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') and warehouse_size <> ''  and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} )
  group by   day , day_of_week  order by  day_of_week 
 `;
}

const QUERY_TREND_BY_USAGE_CREDIT = async (params) => { 
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return `
  select  start_time :: date as date  , count(query_id) as query_count   ,
  sum(total_elapsed_time/1000 * 
    case warehouse_size
      when 'X-Small' then 1/60/60
      when 'Small'   then 2/60/60
      when 'Medium'  then 4/60/60
      when 'Large'   then 8/60/60
      when 'X-Large' then 16/60/60
      when '2X-Large' then 32/60/60
      when '3X-Large' then 64/60/60
      when '4X-Large' then 128/60/60
      else 0
    end  ) as  estimated_credits
  from snowflake.account_usage.query_history  
  where start_time between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')  and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} )
  group by date     
  order by date 
`;
  }

const QUERY_TREND_BY_USAGE_CREDIT_WEEKDAY = async (params) => { 
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return `select   dayname( start_time ) as day    , dayofweek( start_time ) day_of_week ,   
  count(query_id) as query_count   ,
  sum(total_elapsed_time/1000 * 
     case warehouse_size
       when 'X-Small' then 1/60/60
       when 'Small'   then 2/60/60
       when 'Medium'  then 4/60/60
       when 'Large'   then 8/60/60
       when 'X-Large' then 16/60/60
       when '2X-Large' then 32/60/60
       when '3X-Large' then 64/60/60
       when '4X-Large' then 128/60/60
       else 0
     end  ) as  estimated_credits
  from snowflake.account_usage.query_history  
  where start_time between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')  and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} )
  group by day , day_of_week      
  order by  day_of_week
  `;
}

const QUERY_COUNT_BY_SHIFT_HOURS = async (params) => { 
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  const sqlForGroupBy = params.query.fetchBy === 'HOURS' ? ' group by date , shift, shift_no, hours  order by date, hours, shift_no' : 
  ' group by date , shift , shift_no order by date , shift_no';
  const filterQuery = params.query.fetchBy === 'HOURS' ? ' start_time :: date as date ,  hour( start_time ) as hours ' : ' start_time :: date as date';
  const placeholderForHour = params.query.fetchBy === 'HOURS' ? ',hours,' : ',';
  return `with tot_rows as   
  ( select  ${filterQuery} ,  to_char(start_time ,'HH24') as hr_of_day , count(query_id ) as query_count
  from snowflake.account_usage.query_history  
  where start_time between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')  and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} )
  group by  date ${placeholderForHour}  hr_of_day  )
  select  date  ${placeholderForHour} case when hr_of_day between 0 and 8 then 'Morning Shift' 
  when hr_of_day between 8 and 16 then 'Gen Shift' 
  when hr_of_day between 16 and 24 then 'Night Shift' 
  end shift ,   
  case when hr_of_day between 0 and 8 then  1
      when hr_of_day between 8 and 16 then 2 
      when hr_of_day between 16 and 24 then 3 
    end shift_no ,
  sum( query_count ) as query_count 
  from tot_rows 
  ${sqlForGroupBy}
  `;
}

const QUERY_COUNT_BY_USERS = async (params) => {
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return`
  with usercnt as 
  (select user_name ,  count(1) as total_user_queries
  from snowflake.account_usage.query_history  
  where start_time between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')  and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} )
  group by user_name  ) ,
     totcnt as 
     ( select  sum(total_user_queries) as  tot_cnt  from usercnt )     
  select user_name , total_user_queries ,round (( total_user_queries/ tot_cnt ) *100 ,2) percent_cnt 
from  usercnt  ,totcnt
  `;
}

const QUERY_COUNT_BY_WAREHOUSER_NAME_WITH_CR = async (params) => {
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return `
  select warehouse_name , count(query_id) as total_queries ,
  sum(total_elapsed_time/1000 * 
    case warehouse_size
      when 'X-Small' then 1/60/60
      when 'Small'   then 2/60/60
      when 'Medium'  then 4/60/60
      when 'Large'   then 8/60/60
      when 'X-Large' then 16/60/60
      when '2X-Large' then 32/60/60
      when '3X-Large' then 64/60/60
      when '4X-Large' then 128/60/60
      else 0
    end  ) as  estimated_credits
  from snowflake.account_usage.query_history  
  where start_time  between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} ) and warehouse_name <> 'NULL'
  group by warehouse_name
  `;
}

const QUERY_COUNT_BY_WAREHOUSER_SIZE_WITH_CR = async (params) => { 
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return `
  select warehouse_size , count(query_id) as total_queries ,
  sum(total_elapsed_time/1000 * 
        case warehouse_size
          when 'X-Small' then 1/60/60
          when 'Small'   then 2/60/60
          when 'Medium'  then 4/60/60
          when 'Large'   then 8/60/60
          when 'X-Large' then 16/60/60
          when '2X-Large' then 32/60/60
          when '3X-Large' then 64/60/60
          when '4X-Large' then 128/60/60
          else 0
        end  ) as  estimated_credits
  from snowflake.account_usage.query_history  
  where start_time  between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')  and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} ) 
  and  warehouse_size <> 'NULL'
  group by warehouse_size
  `;
}

const QUERY_COUNT_BY_WAREHOUSER_NAME = async (params) => { 
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return `
  select warehouse_name , count(query_id) as total_queries
  from snowflake.account_usage.query_history  
  where start_time  between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} )  and warehouse_name <> 'NULL'
  group by warehouse_name  ;  `;
}

const QUERY_COUNT_BY_WAREHOUSER_SIZE = async (params) => { 
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return `
  select warehouse_size , count(query_id) as total_queries
  from snowflake.account_usage.query_history  
  where start_time between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and TTO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} ) 
  and warehouse_size <> 'NULL'            
  group by warehouse_size   `;
}

const QUERY_COUNT_VS_NUM_OF_USERS = async (params) => { 
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  const sqlForGroupBy = params.query.fetchBy === 'HOURS' ? ' group by date , hours  order by date , hours ' : ' group by date order by date';
  const filterQuery = params.query.fetchBy === 'HOURS' ? ' start_time :: date as date ,  hour( start_time ) as hours ' : ' start_time :: date as date';
  return `
  select  ${filterQuery}   , count ( distinct user_name ) as user_count , count(1) as query_count 
  from snowflake.account_usage.query_history  
  where start_time between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} )
  ${sqlForGroupBy}`;
}

const QUERY_SIZE_VS_EXECUTION_TIME = async (params) => { 
  let sqlForWhere = '';
  sqlForWhere = await fetchWhereCondition(params, sqlForWhere)
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return `
  select query_id ,  bytes_scanned / 1024 / 1024 as query_size ,  TOTAL_ELAPSED_TIME / 1000 as TOTAL_ELAPSED_TIME_secs  
  from   snowflake.account_usage.query_history  
  where start_time between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') ${sqlForWhere} and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} )
  and bytes_scanned <> 0 `;
}

const AVRG_EXECUTION_TIME_BYWAREHOUSE_NAME = async (params) => { 
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return `select warehouse_name ,   avg( TOTAL_ELAPSED_TIME/1000 ) as avg_query_execution 
  from snowflake.account_usage.query_history  
  where start_time between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} ) and warehouse_name <> 'NULL'
  group by warehouse_name
  `;
}

const AVRG_EXECUTION_TIME_BYWAREHOUSE_SIZE = async (params) => { 
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return`select warehouse_size ,   avg( TOTAL_ELAPSED_TIME/1000 ) as avg_query_execution 
  from snowflake.account_usage.query_history  
  where start_time between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')  and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} )
  and  warehouse_size <> 'NULL' 
   group by warehouse_size 
  `;
}

const AVG_EXECUTION_TIME_BY_WAREHOUSER_NAME_WITH_CR = async (params) => { 
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return`select warehouse_name ,   avg( TOTAL_ELAPSED_TIME ) as avg_query_execution ,
  sum(total_elapsed_time/1000 * 
    case warehouse_size
      when 'X-Small' then 1/60/60
      when 'Small'   then 2/60/60
      when 'Medium'  then 4/60/60
      when 'Large'   then 8/60/60
      when 'X-Large' then 16/60/60
      when '2X-Large' then 32/60/60
      when '3X-Large' then 64/60/60
      when '4X-Large' then 128/60/60
      else 0
    end  ) as  estimated_credits
  from snowflake.account_usage.query_history  
  where start_time between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${excludedUsers} )  and warehouse_name <> 'NULL'
  group by warehouse_name
  `;
}

const AVG_EXECUTION_TIME_BY_WAREHOUSER_SIZE_WITH_CR = async (params) => { 
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return `select warehouse_size ,   avg( TOTAL_ELAPSED_TIME ) as avg_query_execution , 
  sum(total_elapsed_time/1000 * 
    case warehouse_size
      when 'X-Small' then 1/60/60
      when 'Small'   then 2/60/60
      when 'Medium'  then 4/60/60
      when 'Large'   then 8/60/60
      when 'X-Large' then 16/60/60
      when '2X-Large' then 32/60/60
      when '3X-Large' then 64/60/60
      when '4X-Large' then 128/60/60
      else 0
    end  ) as  estimated_credits
  from snowflake.account_usage.query_history  
  where start_time between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')  and warehouse_name not in ( ${excludedWarehouse} ) and user_name not in ( ${
