set (coupon1, coupon2, coupon3, coupon4, coupon5, coupon6, coupon7, coupon8, coupon9) = 
    ('val7ehx4', 'val3en9z', 'valk6ltk','X_','X_', 'X_', 'X_','X_','X_');
set (experiment1, experiment2, experiment3, experiment4,experiment5 ) = ('imc_batch_email_push_valentine_launch_nonincentive_canada_20210209','imc_batch_email_push_valentine_launch_incentive_nonexpress11up_20210209',
    'imc_batch_email_push_valentine_launch_incentive_nonexpress2to10_20210209','IMC_batch_email_push_valentine_launch_incentive_nonexpress2_20210209','imc_batch_email_push_valentine_launch_nonincentive_express_20210209');
set segment='experiment';
set exp_table='instadata.dwh.split_test_variants_braze';

WITH 

STV as (
SELECT  stv.user_id,
        experiment,
        variant,
        window_start,
        coalesce(window_end, expires_at, current_date) as window_end,
        case    when fuga.deliveries_lifetime:overall > 10 then 11 
                when fuga.deliveries_lifetime:overall > 2  then 3
                else fuga.deliveries_lifetime:overall
        end as pre_num_purchases,
        case    when fuga.deliveries_lifetime:overall > 0 then 1 
                else 0
        end     as pre_purchase_flag,
        case    when fuga.activation_date_pt is null or fuga.activation_date_pt > window_start::date  then 'A:Act_Null' 
                when timediff(day, fuga.last_visit_date,window_start) >= 30 then 'E:visit_30'
                when timediff(day, fuga.last_visit_date,window_start) >= 14 then 'D:visit_14'
                when timediff(day, fuga.last_visit_date,window_start) >= 7 then 'C:visit_7'
                when timediff(day, fuga.last_visit_date,window_start) >= 0 then 'B:visit_0'
         end    as pre_last_visit,
        coalesce(fuga.is_express_user, 0) as pre_express_flag,
        least(floor(timediff('day',fuga.signup_date_pt, date_trunc(day, convert_timezone('UTC', 'US/Pacific', stv.window_start))::date)/365)+1,7) as tenure,
        last_delivery_warehouse_id as pre_last_delivery_warehouse_id,
        coalesce(last_delivery_region_id,last_visit_region_id) as pre_last_region_id,
        last_visit_platform as pre_last_visit_platform,
        last_visit_order_source as pre_last_visit_order_source
FROM
        (SELECT stv.user_id,
                variant,
                1 as constant,
                created_at as window_start, 
                //nullif(date_trunc('day',window_end),'1970-01-01') as window_end,
                null as window_end,
                experiment
        FROM    identifier($exp_table) stv
        WHERE   experiment in ($experiment1,$experiment2,$experiment3,$experiment4,$experiment5)
        GROUP   BY 1,2,3,4,5,6)  as stv
        LEFT JOIN 
        (SELECT 1 as constant,
                max(date_trunc('day', discount_ends_at)) as expires_at
        FROM    rds_data.discount_policies
        WHERE   lower(discount_code) in ($coupon1,$coupon2,$coupon3,$coupon4,$coupon5,$coupon6,$coupon7,$coupon8,$coupon9)
        GROUP   BY 1) as cp
        ON stv.constant=cp.constant
        LEFT JOIN instadata.dwh.fact_user_growth_accounting as fuga 
            ON stv.user_id = fuga.user_id
            AND dateadd(day,-1, date_trunc(day, convert_timezone('UTC', 'US/Pacific', stv.window_start))::date) = fuga.full_date_pt
        LEFT JOIN instadata.rds_data.users as u 
            ON stv.user_id = u.id
WHERE   1=1
        AND stv.experiment in ($experiment1,$experiment2,$experiment3,$experiment4,$experiment5)
        AND u.roles_mask = 2
        ),
        

TABLE1 AS (
SELECT  stv.user_id,
        variant,
        window_start,
        window_end,
        pre_purchase_flag,
        pre_num_purchases,
        pre_last_visit,
        pre_express_flag,
        tenure,
        pre_last_delivery_warehouse_id,
        pre_last_region_id,
        pre_last_visit_platform,
        pre_last_visit_order_source,
        experiment,
        'Overall' as segment,
        sum(coalesce(specific_cp_costs,0)) as sp_coupon,
        sum(coalesce(oit.order_item_gmv_amt_usd,0)) as gmv,
        sum(case when  di.department_name = 'Floral' then oit.order_item_gmv_amt_usd else 0 end) as gtv,
        count(distinct fod.order_id) as orders,
        max(case when fod.order_id is not null then 1 else 0 end) as purchase_flag,
        count(distinct case when cp2.order_delivery_id is not null then stv.user_id end) as sp_coupnum
FROM    stv as stv
        LEFT JOIN dwh.fact_order_delivery fod 
            ON stv.user_id = fod.user_id
            AND fod.delivery_created_date_time_utc between window_start and window_end
            AND FOD.DELIVERY_STATE = 'delivered' 
            AND FOD.DELIVERY_FINALIZED_IND = 'Y' 
            AND FOD.DELETED_IND = 'N' 
            AND FOD.REDELIVERY_IND = 'N'
        LEFT JOIN  dwh.fact_order_item oit
             ON fod.order_id=oit.order_id
             AND fod.user_id=oit.user_id
        LEFT JOIN dwh.dim_item di 
            ON oit.item_id=di.item_id 
            AND oit.warehouse_id=di.warehouse_id
        left join dwh.dim_product dp2 on di.product_id = dp2.product_id     
        LEFT JOIN rds_data.regions as r 
            ON fod.region_id = r.id
        LEFT JOIN 
            (SELECT order_delivery_id,
                    user_id,
                    sum(case when lower(cp2.coupon_type)='free delivery' then 3.99 else used_value_cents_usd/100 end) as specific_cp_costs
            FROM    rds_data.discount_policies pl
                    inner join 
                    analysts.fact_coupon cp2 on pl.id=cp2.discount_policy_id
            WHERE   lower(discount_code) in ($coupon1,$coupon2,$coupon3,$coupon4,$coupon5,$coupon6,$coupon7,$coupon8, $coupon9) and order_delivery_id is not null
            GROUP   BY 1,2) as cp2
            ON fod.order_delivery_id=cp2.order_delivery_id
            AND fod.user_id=cp2.user_id
        LEFT JOIN analysts.fact_order_delivery_attributes foda
            ON fod.user_id=foda.user_id
            AND fod.order_delivery_id=foda.order_delivery_id
            AND fod.order_id=foda.order_id
        LEFT JOIN dwh.VW_DELIVERY_GTV GTV ON FOD.ORDER_DELIVERY_ID=GTV.ORDER_DELIVERY_ID
GROUP   BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14),

TABLE2 AS (
SELECT  identifier($segment) as group1,
        variant,
        case when lower(variant) like '%control%' then 0 else 1 end as test_flag,
        count(distinct user_id) as cnt,
        
        avg(coalesce(sp_coupon,0)) as sp_coupon,
        avg(coalesce(gmv,0)) as gmv,
        avg(coalesce(gtv,0)) as gtv,
        avg(coalesce(orders,0)) as orders,
        avg(coalesce(purchase_flag,0)) as purchase_flag,
        avg(coalesce(sp_coupnum,0)) as sp_coupnum,
        
        var_samp(sp_coupon) as sd_sp_coupon,
        var_samp(gmv) as sd_gmv,
        var_samp(gtv) as sd_gtv,
        var_samp(orders) as sd_orders,
        var_samp(purchase_flag) as sd_purchase_flag,
        var_samp(sp_coupnum) as sd_sp_coupnum,
        
        min(window_start) as min_start,
        max(window_end) as max_start
        
FROM    TABLE1
GROUP   BY 1,2,3),

TABLE3 AS (
SELECT  cnt,                                
        group1, 
        variant,
        test_flag,
  
        sum(cnt) over (partition by group1) as grand_total,
        cast(first_value(cnt) over (partition by group1 order by test_flag ) as float) as  cnt_c ,  
        cast(first_value(sp_coupon) over (partition by group1 order by test_flag ) as float)  as sp_coupon_c ,
        cast(first_value(gmv) over (partition by group1 order by test_flag ) as float)  as gmv_c ,  
        cast(first_value(gtv) over (partition by group1 order by test_flag ) as float)  as gtv_c ,  
        cast(first_value(orders) over (partition by group1 order by test_flag ) as float) as  orders_c ,
        cast(first_value(purchase_flag) over (partition by group1 order by test_flag ) as float) as  purchase_flag_c ,
        cast(first_value(sp_coupnum) over (partition by group1 order by test_flag ) as float) as  sp_coupnum_c ,
    
        cast(first_value(sd_sp_coupon) over (partition by group1 order by test_flag ) as float)  as sd_sp_coupon_c ,
    
        cast(first_value(sd_gmv) over (partition by group1 order by test_flag ) as float)  as sd_gmv_c ,    
    
        cast(first_value(sd_gtv) over (partition by group1 order by test_flag ) as float)  as sd_gtv_c ,    
        cast(first_value(sd_orders) over (partition by group1 order by test_flag ) as float) as  sd_orders_c ,
        cast(first_value(sd_purchase_flag) over (partition by group1 order by test_flag ) as float) as  sd_purchase_flag_c ,
        cast(first_value(sd_sp_coupnum) over (partition by group1 order by test_flag ) as float) as  sd_sp_coupnum_c ,

        
        
        sp_coupon,
        
        gmv,
        
        gtv,
        
        orders,
        
        purchase_flag,
        sp_coupnum,
        
    
        sd_sp_coupon,
        
        sd_gmv,
        
        sd_gtv,
        
        sd_orders,
        
        sd_purchase_flag,
        sd_sp_coupnum,
                                    
        
        
        min_start,
        max_start
FROM    TABLE2)


SELECT  variant,
        test_flag,  
        group1,
        substr(min_start,1,10) as min_start,
        substr(max_start,1,10) as max_start,
        grand_total,  
        
        cnt,
        cnt_c,

        sp_coupon,
        sp_coupon_c,    
        cast((case when test_flag=1 and sp_coupon_c>0 then (sp_coupon-sp_coupon_c)/sp_coupon_c end)  as decimal (24,5)) 
            as sp_coupon_index, 
        cast((case when test_flag=1 then (sp_coupon-sp_coupon_c) end)  as decimal (24,5)) 
            as sp_coupon_diff,      
        cast((case when test_flag=1 and cnt>1 and cnt_c>1 and sp_coupon>0 and sp_coupon_c>0 then 
            (sp_coupon-sp_coupon_c)/((sqrt(((cnt-1)*sd_sp_coupon+(cnt_c-1)*sd_sp_coupon_c)/(cnt+cnt_c-2)))*(sqrt((1/cnt)+(1/cnt_c)))) end)  as decimal (24,5)) 
            as sp_coupon_sig,   
        cast((case when test_flag=1 then (sp_coupon-sp_coupon_c)*cnt end)  as decimal (24,5)) 
            as sp_coupon_total,

        
        gmv,
        gmv_c,  
        cast((case when test_flag=1 and gmv_c>0 then (gmv-gmv_c)/gmv_c end)  as decimal (24,5)) 
            as gmv_index,   
        cast((case when test_flag=1 then (gmv-gmv_c) end)  as decimal (24,5)) 
            as gmv_diff,        
        cast((case when test_flag=1 and cnt>1 and cnt_c>1 and gmv>0 and gmv_c>0 then 
            (gmv-gmv_c)/((sqrt(((cnt-1)*sd_gmv+(cnt_c-1)*sd_gmv_c)/(cnt+cnt_c-2)))*(sqrt((1/cnt)+(1/cnt_c)))) end)  as decimal (24,5)) 
            as gmv_sig, 
        cast((case when test_flag=1 then (gmv-gmv_c)*cnt end)  as decimal (24,5)) 
            as gmv_total,
            
        
        gtv,
        gtv_c,  
        cast((case when test_flag=1 and gtv_c>0 then (gtv-gtv_c)/gtv_c end)  as decimal (24,5)) 
            as gtv_index,   
        cast((case when test_flag=1 then (gtv-gtv_c) end)  as decimal (24,5)) 
            as gtv_diff,        
        cast((case when test_flag=1 and cnt>1 and cnt_c>1 and gtv>0 and gtv_c>0 then 
            (gtv-gtv_c)/((sqrt(((cnt-1)*sd_gtv+(cnt_c-1)*sd_gtv_c)/(cnt+cnt_c-2)))*(sqrt((1/cnt)+(1/cnt_c)))) end)  as decimal (24,5)) 
            as gtv_sig, 
        cast((case when test_flag=1 then (gtv-gtv_c)*cnt end)  as decimal (24,5)) 
            as gtv_total,
            
        
        orders,
        orders_c,   
        cast((case when test_flag=1 and orders_c>0 then (orders-orders_c)/orders_c end)  as decimal (24,5)) 
            as orders_index,    
        cast((case when test_flag=1 then (orders-orders_c) end)  as decimal (24,5)) 
            as orders_diff,     
        cast((case when test_flag=1 and cnt>1 and cnt_c>1 and orders>0 and orders_c>0 then 
            (orders-orders_c)/((sqrt(((cnt-1)*sd_orders+(cnt_c-1)*sd_orders_c)/(cnt+cnt_c-2)))*(sqrt((1/cnt)+(1/cnt_c)))) end)  as decimal (24,5)) 
            as orders_sig,  
        cast((case when test_flag=1 then (orders-orders_c)*cnt end)  as decimal (24,5)) 
            as orders_total,
            
        
        sp_coupnum,
        sp_coupnum_c,   
        cast((case when test_flag=1 and sp_coupnum_c>0 then (sp_coupnum-sp_coupnum_c)/sp_coupnum_c end)  as decimal (24,5)) 
            as sp_coupnum_index,    
        cast((case when test_flag=1 then (sp_coupnum-sp_coupnum_c) end)  as decimal (24,5)) 
            as sp_coupnum_diff,     
        cast((case when test_flag=1 and cnt>1 and cnt_c>1 and sp_coupnum>0 and sp_coupnum_c>0 then 
            (sp_coupnum-sp_coupnum_c)/((sqrt(((cnt-1)*sd_sp_coupnum+(cnt_c-1)*sd_sp_coupnum_c)/(cnt+cnt_c-2)))*(sqrt((1/cnt)+(1/cnt_c)))) end)  as decimal (24,5)) 
            as sp_coupnum_sig,  
        cast((case when test_flag=1 then (sp_coupnum-sp_coupnum_c)*cnt end)  as decimal (24,5)) 
            as sp_coupnum_total,
            
        
        
        purchase_flag,
        purchase_flag_c,    
        cast((case when test_flag=1 and purchase_flag_c>0 then (purchase_flag-purchase_flag_c)/purchase_flag_c end)  as decimal (24,5)) 
            as purchase_flag_index, 
        cast((case when test_flag=1 then (purchase_flag-purchase_flag_c) end)  as decimal (24,5)) 
            as purchase_flag_diff,      
        cast((case when test_flag=1 and cnt>1 and cnt_c>1 and purchase_flag>0 and purchase_flag_c>0 then 
            (purchase_flag-purchase_flag_c)/((sqrt(((cnt-1)*sd_purchase_flag+(cnt_c-1)*sd_purchase_flag_c)/(cnt+cnt_c-2)))*(sqrt((1/cnt)+(1/cnt_c)))) end)  as decimal (24,5)) 
            as purchase_flag_sig,   
        cast((case when test_flag=1 then (purchase_flag-purchase_flag_c)*cnt end)  as decimal (24,5)) 
            as purchase_flag_total
                                                    
FROM    TABLE3  
WHERE   test_flag=1
ORDER   BY group1, test_flag desc, variant  ;
