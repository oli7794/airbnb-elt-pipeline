
-- Part 4 a
-- Top 3 performing LGAs

with
    lga_revenue as (
        select
            lc.lga_code,
            sum(
                case
                    when f.has_availability = true
                    then (30 - f.availability_30) * f.price
                end
            ) as estimated_revenue,
            count(
                case when f.has_availability = true then f.listing_id end
            ) as active_listings,
            (
                sum(
                    case
                        when f.has_availability = true
                        then (30 - f.availability_30) * f.price
                    end
                ) / nullif(
                    count(case when f.has_availability = true then f.listing_id end), 0
                )
            ) as revenue_per_active_listing
        from silver.s_facts f
        join
            silver.facts_snapshot fs
            on f.listing_id = fs.listing_id
            and f.host_id = fs.host_id
        join
            silver.s_lga_suburb ls
            on lower(fs.listing_neighbourhood) = lower(ls.suburb_name)
        join silver.s_lga_code lc on lower(ls.lga_name) = lower(lc.lga_name)
        group by lc.lga_code
    ),
    top_3_lga as (
        select
            lga_code,
            to_char(
                revenue_per_active_listing, 'fm$999,999.00'
            ) as revenue_per_active_listing
        from lga_revenue
        order by revenue_per_active_listing desc
        limit 3
    ),
    demographic_data as (
        select
            g01.lga_code,
            g01.age_0_4_total,
            g01.age_5_14_total,
            g01.age_15_19_total,
            g01.age_20_24_total,
            g01.age_25_34_total,
            g01.age_35_44_total,
            g01.age_45_54_total,
            g01.age_55_64_total,
            g01.age_65_74_total,
            g01.age_75_84_total,
            g01.age_85_over_total,
            g02.average_household_size,
            g02.median_age_persons,
            g02.median_tot_hhd_inc_weekly
        from silver.s_census_data_g01 g01
        join silver.s_census_data_g02 g02 on g01.lga_code = g02.lga_code
    ),
    top_3_lga_demographics as (
        select d.*, t.revenue_per_active_listing
        from demographic_data d
        join top_3_lga t on d.lga_code = t.lga_code
    )
select *
from top_3_lga_demographics
;



-- Bottom 3 performing LGAs

with
    lga_revenue as (
        select
            lc.lga_code,
            sum(
                case
                    when f.has_availability = true
                    then (30 - f.availability_30) * f.price
                end
            ) as estimated_revenue,
            count(
                case when f.has_availability = true then f.listing_id end
            ) as active_listings,
            (
                sum(
                    case
                        when f.has_availability = true
                        then (30 - f.availability_30) * f.price
                    end
                ) / nullif(
                    count(case when f.has_availability = true then f.listing_id end), 0
                )
            ) as revenue_per_active_listing
        from silver.s_facts f
        join
            silver.facts_snapshot fs
            on f.listing_id = fs.listing_id
            and f.host_id = fs.host_id
        join
            silver.s_lga_suburb ls
            on lower(fs.listing_neighbourhood) = lower(ls.suburb_name)
        join silver.s_lga_code lc on lower(ls.lga_name) = lower(lc.lga_name)
        group by lc.lga_code  -- Group by LGA code
    ),
    bottom_3_lga as (
        select
            lga_code,
            to_char(
                revenue_per_active_listing, 'FM$999,999.00'
            ) as revenue_per_active_listing
        from lga_revenue
        order by revenue_per_active_listing asc
        limit 3
    ),
    demographic_data as (
        select
            g01.lga_code,
            g01.age_0_4_total,
            g01.age_5_14_total,
            g01.age_15_19_total,
            g01.age_20_24_total,
            g01.age_25_34_total,
            g01.age_35_44_total,
            g01.age_45_54_total,
            g01.age_55_64_total,
            g01.age_65_74_total,
            g01.age_75_84_total,
            g01.age_85_over_total,
            g02.average_household_size,
            g02.median_age_persons,
            g02.median_tot_hhd_inc_weekly
        from silver.s_census_data_g01 g01
        join silver.s_census_data_g02 g02 on g01.lga_code = g02.lga_code  -- Link census data by lga_code
    ),
    bottom_3_lga_demographics as (
        select d.*, t.revenue_per_active_listing
        from demographic_data d
        join bottom_3_lga t on d.lga_code = t.lga_code  -- Match lga_code with bottom 3 performing LGAs
    )
select *
from bottom_3_lga_demographics
;


-- Part 4 b
with
    lga_revenue as (
        select
            l.lga_name,
            sum(
                case
                    when f.has_availability = true
                    then (30 - f.availability_30) * f.price
                end
            ) as estimated_revenue,
            count(
                case when f.has_availability = true then f.listing_id end
            ) as active_listings,
            (
                sum(
                    case
                        when f.has_availability = true
                        then (30 - f.availability_30) * f.price
                    end
                ) / nullif(
                    count(case when f.has_availability = true then f.listing_id end), 0
                )
            ) as revenue_per_active_listing
        from silver.s_facts f
        join silver.facts_snapshot fs on f.host_id = fs.host_id
        join
            silver.s_lga_suburb l
            on lower(fs.listing_neighbourhood) = lower(l.suburb_name)
        group by l.lga_name
    ),
    lga_median_age as (
        select lc.lga_name, g02.median_age_persons
        from silver.s_census_data_g02 g02
        join silver.s_lga_code lc on g02.lga_code = lc.lga_code
    ),
    lga_data as (
        select
            lr.lga_name,
            to_char(lr.revenue_per_active_listing, 'FM$999,999.00'),
            la.median_age_persons
        from lga_revenue lr
        join lga_median_age la on lr.lga_name = la.lga_name
    )
select *
from lga_data
;



-- Part 4 c
with neighbourhood_revenue as (
    select 
        fs.listing_neighbourhood,
        sum(case when f.has_availability = true then (30 - f.availability_30) * f.price end) as estimated_revenue,
        count(case when f.has_availability = true then f.listing_id end) as active_listings,
        (sum(case when f.has_availability = true then (30 - f.availability_30) * f.price end) / 
            nullif(count(case when f.has_availability = true then f.listing_id end), 0)) as revenue_per_active_listing
    from silver.s_facts f
    join silver.facts_snapshot fs on f.host_id = fs.host_id 
    group by fs.listing_neighbourhood  
),
top_5_neighbourhoods as (
    select 
        listing_neighbourhood,
        revenue_per_active_listing
    from neighbourhood_revenue
    order by revenue_per_active_listing desc
    limit 5
),
neighbourhood_stays as (
    select 
        fs.listing_neighbourhood,
        fs.property_type,
        fs.room_type,
        f.accommodates,
        sum(30 - f.availability_30) as total_stays,
        nr.revenue_per_active_listing
    from silver.s_facts f
    join silver.facts_snapshot fs on f.listing_id = fs.listing_id
    join top_5_neighbourhoods nr on fs.listing_neighbourhood = nr.listing_neighbourhood
    group by fs.listing_neighbourhood, fs.property_type, fs.room_type, f.accommodates, nr.revenue_per_active_listing
),
best_listings as (
    select
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        total_stays,
        revenue_per_active_listing,
        rank() over (partition by listing_neighbourhood order by total_stays desc) as stay_rank
    from neighbourhood_stays
)
select 
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    total_stays,
    TO_CHAR(revenue_per_active_listing, 'FM$999,999.00') as revenue_per_active_listing
from best_listings
where stay_rank = 1
order by listing_neighbourhood;



-- Part 4 d

with host_listing_lga as (
    select 
        f.host_id,
        l.lga_code,
        count(distinct f.listing_id) as total_listings
    from silver.s_facts f
    join silver.facts_snapshot fs on f.listing_id = fs.listing_id 
    join silver.s_lga_suburb s on lower(fs.listing_neighbourhood) = lower(s.suburb_name)
    join silver.s_lga_code l on lower(s.lga_name) = lower(l.lga_name)
    group by f.host_id, l.lga_code
),
host_lga_distribution as (
    select
        host_id,
        count(distinct lga_code) as distinct_lgas,
        sum(total_listings) as total_listings
    from host_listing_lga
    group by host_id
),
concentration_analysis as (
    select
        host_id,
        total_listings,
        distinct_lgas,
        case 
            when distinct_lgas = 1 then 'same lga'
            else 'multiple lgas'
        end as lga_distribution
    from host_lga_distribution
)
select 
    lga_distribution,
    count(host_id) as host_count,
    sum(total_listings) as total_listings
from concentration_analysis
group by lga_distribution;



-- Part 4 e


with single_listing_hosts as (
    select 
        f.host_id,
        f.listing_id,
        sum(case when f.has_availability = true then (30 - f.availability_30) * f.price end) as estimated_revenue_12m,
        l.lga_code,
        fs.listing_neighbourhood
    from silver.s_facts f
    join silver.facts_snapshot fs on f.listing_id = fs.listing_id
    join silver.s_lga_suburb s on lower(fs.listing_neighbourhood) = lower(s.suburb_name)
    join silver.s_lga_code l on lower(s.lga_name) = lower(l.lga_name)
    group by f.host_id, f.listing_id, l.lga_code, fs.listing_neighbourhood
    having count(f.listing_id) = 1
),
lga_mortgage as (
    select 
        g02.lga_code,
        g02.median_mortgage_repay_monthly * 12 as annual_median_mortgage_repay
    from silver.s_census_data_g02 g02
),
revenue_vs_mortgage as (
    select 
        h.host_id,
        h.listing_id,
        h.listing_neighbourhood,
        h.estimated_revenue_12m,
        m.annual_median_mortgage_repay,
        case 
            when h.estimated_revenue_12m >= m.annual_median_mortgage_repay then 'Yes'
            else 'No'
        end as can_cover_mortgage
    from single_listing_hosts h
    join lga_mortgage m on h.lga_code = m.lga_code
)
select host_id, listing_id, listing_neighbourhood, estimated_revenue_12m, annual_median_mortgage_repay, can_cover_mortgage
from revenue_vs_mortgage
order by host_id;


with single_listing_hosts as (
    select 
        f.host_id,
        f.listing_id,
        sum(case when f.has_availability = true then (30 - f.availability_30) * f.price end) as estimated_revenue_12m,
        l.lga_code
    from silver.s_facts f
    join silver.facts_snapshot fs on f.listing_id = fs.listing_id
    join silver.s_lga_suburb s on lower(fs.listing_neighbourhood) = lower(s.suburb_name)
    join silver.s_lga_code l on lower(s.lga_name) = lower(l.lga_name)
    group by f.host_id, f.listing_id, l.lga_code
    having count(f.listing_id) = 1
),
lga_mortgage as (
    select 
        g02.lga_code,
        g02.median_mortgage_repay_monthly * 12 as annual_median_mortgage_repay
    from silver.s_census_data_g02 g02
),
revenue_vs_mortgage as (
    select 
        h.lga_code,
        h.host_id,
        h.estimated_revenue_12m,
        m.annual_median_mortgage_repay,
        case 
            when h.estimated_revenue_12m >= m.annual_median_mortgage_repay then 1
            else 0
        end as can_cover_mortgage
    from single_listing_hosts h
    join lga_mortgage m on h.lga_code = m.lga_code
),
lga_coverage_percentage as (
    select
        r.lga_code,
        count(r.host_id) as total_hosts,
        sum(r.can_cover_mortgage) as hosts_covering_mortgage,
        to_char((sum(r.can_cover_mortgage)::float / count(r.host_id)::float) * 100, 'FM999990.00') || '%' as coverage_percentage
    from revenue_vs_mortgage r
    group by r.lga_code
)
select lga_code, coverage_percentage
from lga_coverage_percentage
order by coverage_percentage desc
limit 1;






