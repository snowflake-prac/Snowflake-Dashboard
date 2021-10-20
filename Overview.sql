--test
const AppConstant = require('../../constant/app-constant');

const COST_CARD_DETAILS = async (params) => {
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let costPerCredit = params.query.creditValue ? params.query.creditValue : AppConstant.COST_PER_CREDIT
  return `with 
    date_val as 
      ( select fromdt , todt , 
          dateadd('day', - ( datediff + 1)  ,fromdt ) last_fromdt ,
          dateadd('day' , -1 ,fromdt )  last_todt 
      from ( 
      select TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') fromdt ,
      TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') todt ,
            datediff( 'day', fromdt ,todt ) datediff  )  )  
          
    select curr_tot_credits_used * ${costPerCredit} as curr_tot_credits_used, 
          case when curr_tot_credits_used > 0 
                then nvl(( curr_tot_credits_used - last_tot_credits_used ) * 100 / curr_tot_credits_used , 0) else 0 end as  tot_credits_perc_increase,
          (curr_tot_credits_used/datediff(day,(select to_date(fromdt) from date_val )-1, (select to_date(todt) from date_val ) )) * ${costPerCredit} as curr_avg_credits_used
          ,case when curr_avg_credits_used > 0 
                then nvl ( ( curr_avg_credits_used - last_tot_credits_used/datediff(day,(select to_date(last_fromdt) from date_val )-1, (select to_date(last_todt) from date_val ) ) ) * 100 / curr_avg_credits_used , 0 ) else 0 end as  avg_credits_perc_increase
    from ( select 
          ( select   nvl( sum(credits_used) ,0)
            from snowflake.account_usage.warehouse_metering_history   
            where to_date(start_time) between  (select last_fromdt from date_val )  
                and (select last_todt from date_val ) 
            and warehouse_name not in ( ${excludedWarehouse} )   )  as last_tot_credits_used ,
          ( select    nvl( sum(credits_used) ,0) 
            from snowflake.account_usage.warehouse_metering_history   
            where to_date(start_time) between  (select fromdt from date_val )  
              and (select todt from date_val ) 
            and warehouse_name not in ( ${excludedWarehouse} )    ) as curr_tot_credits_used 
        )
  `
};

const PERFORMANCE_QUERY_REPORT =  async(params) => {
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return `with 
  date_val as 
    ( select fromdt , todt , 
         dateadd('day', - ( datediff + 1)  ,fromdt ) last_fromdt ,
         dateadd('day' , -1 ,fromdt )  last_todt 
    from ( 
    select TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') fromdt ,
    TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') todt ,
           datediff( 'day', fromdt ,todt ) datediff  )  )   
         
    select curr_max_execution_time /1000 as  curr_max_execution_time_Secs , 
       case  when curr_max_execution_time > 0 
           then nvl( ( curr_max_execution_time - last_max_execution_time ) * 100 / curr_max_execution_time ,0 ) else 0 end  as  max_execution_time_perc_increase,
        curr_median_execution_time /1000 as  curr_median_execution_time_Secs , 
       case when curr_median_execution_time > 0 
           then nvl (( curr_median_execution_time - last_median_execution_time ) * 100 / curr_median_execution_time ,0 ) else 0 end  as  median_execution_time_perc_increase,
           current_tot_queries , 
      case when current_tot_queries >0 
          then nvl (( current_tot_queries - last_tot_queries ) * 100 / current_tot_queries , 0 ) else 0 end  as  tot_queries_perc_increase
           
     from ( select * from 
      (select  nvl( max(TOTAL_ELAPSED_TIME) ,0 ) as last_max_execution_time  ,nvl( median(TOTAL_ELAPSED_TIME) ,0) as last_median_execution_time, nvl( count(query_id) ,0) as last_tot_queries
          from snowflake.account_usage.query_history  
          where to_date(start_time) between  (select last_fromdt from date_val )  
                and (select last_todt from date_val ) 
              and warehouse_name not in ( ${excludedWarehouse} )  
          and user_name not in ( ${excludedUsers} ) 
            )  as last_time ,
       ( select  nvl( max(TOTAL_ELAPSED_TIME) ,0) as curr_max_execution_time,nvl( median(TOTAL_ELAPSED_TIME) ,0) as curr_median_execution_time , nvl( count(query_id) ,0) as current_tot_queries
          from snowflake.account_usage.query_history  
          where to_date(start_time) between  (select fromdt from date_val )  
         and (select todt from date_val ) 
       and warehouse_name not in ( ${excludedWarehouse} ) 
          and user_name not in ( ${excludedUsers} ) 
         ) as curr_time
       )`
 };

const DATES_WITH_HIGHEST_USAGE_COST =  async(params) => {
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''"; 
  return `select date , credits_used ,rank() over (   order by credits_used desc ) credit_rank 
    from (  select   start_time :: date as  date  ,sum( credits_used ) credits_used   
    from snowflake.account_usage.warehouse_metering_history wmh 
    where wmh.start_time between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
    and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
    and warehouse_name not in ( ${excludedWarehouse} )              
    group by date   ) 
    qualify credit_rank <= 5
    order by date
  `;
};

const USERS_WITH_HIGHEST_USAGE_COST = async(params) => {
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
    return `select user_name  , estimated_credits , credit_rank , ( estimated_credits / tot_credits ) * 100 as perc_credits from (
      select user_name  , estimated_credits , 
              rank() over (   order by estimated_credits desc ) credit_rank ,
          sum( estimated_credits ) over() as tot_credits 
       from ( select user_name, sum(total_elapsed_time/1000 * 
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
                        end) as  estimated_credits
         from snowflake.account_usage.query_history
         where start_time  between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')
                       and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
         and warehouse_name not in ( ${excludedWarehouse} ) 
             and user_name not in ( ${excludedUsers} )                
          group by user_name  ) ) 
         qualify credit_rank <= 5
         order by credit_rank`
};

const QUERY_TIME_TREND = async(params) => {
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  const sqlForGroupBy = params.query.fetchBy === 'HOURS' ? ' group by date , hours  order by date , hours ' : ' group by date order by date';
  const filterQuery = params.query.fetchBy === 'HOURS' ? ' start_time :: date as date ,  hour( start_time ) as hours ' : ' start_time :: date as date';
  return `  select  ${filterQuery} ,
  count(1) as total_queries , 
  avg(TOTAL_ELAPSED_TIME)/1000 as Execution_Time_seconds     
  from snowflake.account_usage.query_history 
  where  start_time  between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')  
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')
  and warehouse_size <> ''  
  and warehouse_name not in ( ${excludedWarehouse} )  
  and user_name not in ( ${excludedUsers} )  ${sqlForGroupBy}`
};

const WAREHOUSE_BY_USAGE_NAME = async(params) => {
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  return ` select warehouse_name,sum(credits_used) estimated_credits from snowflake.account_usage.warehouse_metering_history 
  where start_time  between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')  
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')
  and warehouse_name not in ( ${excludedWarehouse} )  
  and warehouse_name<>'CLOUD_SERVICES_ONLY'
  group by warehouse_name
  having sum(credits_used) <>0
  order by estimated_credits desc
  `
};

const WAREHOUSE_BY_USAGE_SIZE = async(params) => { 
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  return `select warehouse_size,case warehouse_size
  when 'X-Small' then 1
  when 'Small'   then 2
  when 'Medium'  then 3
  when 'Large'   then 4
  when 'X-Large' then 5
  when '2X-Large' then 6
  when '3X-Large' then 7
  when '4X-Large' then 8
  else 0
  end  as Sr_no,sum(credits_used) as estimated_credits from snowflake.account_usage.warehouse_metering_history a
  left join (select warehouse_name,max(warehouse_size) warehouse_size from snowflake.account_usage.QUERY_HISTORY
  group by warehouse_name) b on a.warehouse_name=b.warehouse_name
  where  start_time  between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')  
  and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and warehouse_size <> ''
  and a.warehouse_name not in ( ${excludedWarehouse} )  
  group by warehouse_size,Sr_no
  order by Sr_no
  `
};

const QUERY_TIME_BY_WAREHOUSE_SIZE = async(params) => {
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return `select warehouse_size , 
  avg(total_elapsed_time) /1000  as Execution_Time_secs   ,
  avg( QUEUED_OVERLOAD_TIME)/1000 as  Queued_Time_Secs ,
    count(1) as total_queries ,
    case warehouse_size
    when 'X-Small' then 1
    when 'Small'   then 2
    when 'Medium'  then 3
    when 'Large'   then 4
    when 'X-Large' then 5
    when '2X-Large' then 6
    when '3X-Large' then 7
    when '4X-Large' then 8
    else 0
  end  as Sr_no 
  from snowflake.account_usage.query_history  
  where  start_time  between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')  
                and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and warehouse_size <> ''
  and warehouse_name not in ( ${excludedWarehouse} )  
  and user_name not in ( ${excludedUsers} ) 
  group by warehouse_size  order by Sr_no`
};

const TOP_QUERIES_BY_COST_AND_EXECUTION_TIME = async(params) => {
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return ` select user_name , query_id , WAREHOUSE_NAME , estimated_credits , no_of_records ,
  Execution_Time_seconds ,cpu_utilization , query , start_time, end_time,
  rank() over (  order by estimated_credits desc ) credit_rank 
  from (  select  USER_NAME , query_id ,WAREHOUSE_NAME , ROWS_PRODUCED no_of_records , start_time, end_time,
  TOTAL_ELAPSED_TIME/1000 as Execution_Time_seconds ,QUERY_LOAD_PERCENT  as cpu_utilization ,
  QUERY_TEXT as query ,
  total_elapsed_time/1000 * 
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
        end  as estimated_credits  
  from snowflake.account_usage.query_history 
  where  start_time  between TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi')  
              and TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') 
  and warehouse_name not in ( ${excludedWarehouse} )  
  and user_name not in ( ${excludedUsers} )                 )
  qualify  credit_rank <= 10                    
  order by credit_rank`
};

const COST_CARD_TREND_LINE = async(params) => {
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let costPerCredit = params.query.creditValue ? params.query.creditValue : AppConstant.COST_PER_CREDIT;
  return `with 
  date_val as 
    ( select fromdt , todt , 
      dateadd('day', - ( datediff * 5 + 1)  ,fromdt ) last_fromdt ,
      dateadd('day' , -1 ,fromdt )  last_todt 
    from ( 
    select TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') fromdt ,
    TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') todt ,
        datediff( 'day', fromdt ,todt ) datediff  )  )               
    select start_time :: date as date  , (nvl( sum(credits_used) ,0)) * ${costPerCredit} AS  credits_used,
    (nvl( AVG(credits_used) ,0)) * ${costPerCredit} AS  avg_credits_used
    from snowflake.account_usage.warehouse_metering_history   
    where start_time between  (select last_fromdt from date_val )  
                  and (select todt from date_val ) 
    and warehouse_name not in ( ${excludedWarehouse} )                
    GROUP BY start_time :: date order by date 
  `
};

const PERFORMANCE_QUERY_REPORT_TREND_LINE = async(params) => {
  let excludedWarehouse = params.query.excludedWarehouses? params.query.excludedWarehouses : "''";
  let excludedUsers = params.query.excludedUserNames? params.query.excludedUserNames : "''";
  return `with 
  date_val as 
    ( select fromdt , todt , 
       dateadd('day', - ( datediff * 5 + 1)  ,fromdt ) last_fromdt ,
       dateadd('day' , -1 ,fromdt )  last_todt 
    from ( 
    select TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') fromdt ,
    TO_TIMESTAMP( ?, 'mm/dd/yyyy hh24:mi') todt ,
         datediff( 'day', fromdt ,todt ) datediff  )  )              
     select start_time :: date as date  , nvl( MAX( TOTAL_ELAPSED_TIME ) ,0)/1000  AS MAX_ELAPSED_TIME_sECS,nvl( median(TOTAL_ELAPSED_TIME) ,0)/1000  AS Median_ELAPSED_TIME_sECS,nvl( count(query_id) ,0)  AS  Query_cnt
    from snowflake.account_usage.query_history  
     where start_time between  (select last_fromdt from date_val )  
                  and (select todt from date_val )   
      and warehouse_name not in ( ${excludedWarehouse} )  
    and user_name not in ( ${excludedUsers})              
    GROUP BY start_time :: date order by date;
  `;
}

const APP_INITIAL_DATA = `select distinct null users,warehouse_name,warehouse_size from snowflake.account_usage.query_history where warehouse_size is not null
union
select distinct name,null,null from snowflake.account_usage.USERS`;

const TIMEZONE = `show parameters like 'timezone'`;

module.exports = {
  COST_CARD_DETAILS:COST_CARD_DETAILS,
  PERFORMANCE_QUERY_REPORT:PERFORMANCE_QUERY_REPORT,
  DATES_WITH_HIGHEST_USAGE_COST: DATES_WITH_HIGHEST_USAGE_COST,
  USERS_WITH_HIGHEST_USAGE_COST: USERS_WITH_HIGHEST_USAGE_COST,
  QUERY_TIME_TREND: QUERY_TIME_TREND,
  WAREHOUSE_BY_USAGE_NAME: WAREHOUSE_BY_USAGE_NAME,
  WAREHOUSE_BY_USAGE_SIZE: WAREHOUSE_BY_USAGE_SIZE,
  QUERY_TIME_BY_WAREHOUSE_SIZE: QUERY_TIME_BY_WAREHOUSE_SIZE,
  TOP_QUERIES_BY_COST_AND_EXECUTION_TIME: TOP_QUERIES_BY_COST_AND_EXECUTION_TIME,
  COST_CARD_TREND_LINE: COST_CARD_TREND_LINE,
  PERFORMANCE_QUERY_REPORT_TREND_LINE:PERFORMANCE_QUERY_REPORT_TREND_LINE,
  APP_INITIAL_DATA:APP_INITIAL_DATA,
  TIMEZONE: TIMEZONE
}