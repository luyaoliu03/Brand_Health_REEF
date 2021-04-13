WITH uptime_7 AS (
	WITH uptime_7_pct_rank AS (
		WITH uptime_agg AS (
		    WITH uptime_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
					 , brand
					 , brand_type
					 , SUM(downtime) AS downtime_sum
					 , SUM(mins_open) AS mins_open_sum
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_7_days_begin
				 , a.brand
				 , a.brand_type
			     , CASE
				    WHEN SUM(b.mins_open_sum) = 0 THEN NULL ELSE ( 1 - SUM(b.downtime_sum)/ SUM(b.mins_open_sum) ) END AS last_7_days_value
			FROM uptime_basics a
			LEFT JOIN uptime_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3,4
							)

	    SELECT *
			 , RANK() OVER (PARTITION BY order_date ORDER BY last_7_days_value) AS last_7_days_rank
		FROM uptime_agg
		WHERE last_7_days_value IS NOT NULL
								)
    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_7_days_value
         , (a.last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_7_days_score
    FROM uptime_7_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_7_days_rank) AS number_of_rows
        FROM uptime_7_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

uptime_28 AS (
	WITH uptime_28_pct_rank AS (
		WITH uptime_agg AS (
		    WITH uptime_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
					 , brand
					 , brand_type
					 , SUM(downtime) AS downtime_sum
					 , SUM(mins_open) AS mins_open_sum
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_28_days_begin
				 , a.brand
				 , a.brand_type
			     , CASE
				    WHEN SUM(b.mins_open_sum) = 0 THEN NULL ELSE ( 1 - SUM(b.downtime_sum)/ SUM(b.mins_open_sum) ) END AS last_28_days_value
			FROM uptime_basics a
			LEFT JOIN uptime_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3,4
							)

	    SELECT *
			 , RANK() OVER (PARTITION BY order_date ORDER BY last_28_days_value) AS last_28_days_rank
		FROM uptime_agg
		WHERE last_28_days_value IS NOT NULL
								)
    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_28_days_value
         , (a.last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_28_days_score
    FROM uptime_28_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_28_days_rank) AS number_of_rows
        FROM uptime_28_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

defect_rate_7 AS (
	WITH defect_7_pct_rank AS (
		WITH defect_agg AS (
		    WITH defect_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
					 , brand
					 , brand_type
					 , SUM(refunds) AS refunds_sum
					 , SUM(cancelled_gmv) AS cancelled_gmv_sum
				     , SUM(unfulfilled_gmv) AS unfulfilled_gmv_sum
				     , SUM(subtotal) AS subtotal_sum
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_7_days_begin
				 , a.brand
				 , a.brand_type
			     , CASE
				    WHEN SUM(b.subtotal_sum) = 0 THEN NULL ELSE (sum(b.refunds_sum) + sum(b.cancelled_gmv_sum) + sum(b.unfulfilled_gmv_sum))/sum(b.subtotal_sum) END AS last_7_days_value
			FROM defect_basics a
			LEFT JOIN defect_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3,4
						   )

		SELECT *
		     , RANK() OVER (PARTITION BY order_date ORDER BY last_7_days_value DESC) AS last_7_days_rank
		FROM defect_agg
		WHERE last_7_days_value IS NOT NULL
	                        )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_7_days_value
         , (a.last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_7_days_score
    FROM defect_7_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_7_days_rank) AS number_of_rows
        FROM defect_7_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

defect_rate_28 AS (
	WITH defect_28_pct_rank AS (
		WITH defect_agg AS (
		    WITH defect_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
					 , brand
					 , brand_type
					 , SUM(refunds) AS refunds_sum
					 , SUM(cancelled_gmv) AS cancelled_gmv_sum
				     , SUM(unfulfilled_gmv) AS unfulfilled_gmv_sum
				     , SUM(subtotal) AS subtotal_sum
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_28_days_begin
				 , a.brand
				 , a.brand_type
			     , CASE
				    WHEN SUM(b.subtotal_sum) = 0 THEN NULL ELSE (sum(b.refunds_sum) + sum(b.cancelled_gmv_sum) + sum(b.unfulfilled_gmv_sum))/sum(b.subtotal_sum) END AS last_28_days_value
			FROM defect_basics a
			LEFT JOIN defect_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3,4
						   )

		SELECT *
		     , RANK() OVER (PARTITION BY order_date ORDER BY last_28_days_value DESC) AS last_28_days_rank
		FROM defect_agg
		WHERE last_28_days_value IS NOT NULL
	                        )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_28_days_value
         , (a.last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_28_days_score
    FROM defect_28_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_28_days_rank) AS number_of_rows
        FROM defect_28_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

rating_7 AS (
	WITH rating_7_pct_rank AS (
		WITH rating_agg AS (
		    WITH rating_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
				     , brand
					 , brand_type
					 , AVG(last_rating) AS avg_rating
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_7_days_begin
				 , a.brand
				 , a.brand_type
			     , AVG(b.avg_rating) AS last_7_days_value
			FROM rating_basics a
			LEFT JOIN rating_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3,4
						)

		SELECT *
		     , RANK() OVER (PARTITION BY order_date ORDER BY last_7_days_value) AS last_7_days_rank
		FROM rating_agg
	    WHERE last_7_days_value IS NOT NULL
	                            )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_7_days_value
         , (a.last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_7_days_score
    FROM rating_7_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_7_days_rank) AS number_of_rows
        FROM rating_7_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

rating_28 AS (
	WITH rating_28_pct_rank AS (
		WITH rating_agg AS (
		    WITH rating_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
				     , brand
					 , brand_type
					 , AVG(last_rating) AS avg_rating
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_28_days_begin
				 , a.brand
				 , a.brand_type
			     , AVG(b.avg_rating) AS last_28_days_value
			FROM rating_basics a
			LEFT JOIN rating_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3,4
						)

		SELECT *
		     , RANK() OVER (PARTITION BY order_date ORDER BY last_28_days_value) AS last_28_days_rank
		FROM rating_agg
	    WHERE last_28_days_value IS NOT NULL
	                            )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_28_days_value
         , (a.last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_28_days_score
    FROM rating_28_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_28_days_rank) AS number_of_rows
        FROM rating_28_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

positioning_7 AS (
	WITH positioning_7_pct_rank AS (
		WITH positioning_agg AS (
		    WITH positioning_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
				     , brand
					 , brand_type
					 , AVG(avg_page_ranking) AS avg_positioning
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_7_days_begin
				 , a.brand
				 , a.brand_type
			     , AVG(b.avg_positioning) AS last_7_days_value
			FROM positioning_basics a
			LEFT JOIN positioning_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3,4
								)

		SELECT *
		     , RANK() OVER (PARTITION BY order_date ORDER BY last_7_days_value DESC) AS last_7_days_rank
		FROM positioning_agg
	    WHERE last_7_days_value IS NOT NULL
	                                )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_7_days_value
         , (a.last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_7_days_score
    FROM positioning_7_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_7_days_rank) AS number_of_rows
        FROM positioning_7_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

positioning_28 AS (
	WITH positioning_28_pct_rank AS (
		WITH positioning_agg AS (
		    WITH positioning_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
				     , brand
					 , brand_type
					 , AVG(avg_page_ranking) AS avg_positioning
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_28_days_begin
				 , a.brand
				 , a.brand_type
			     , AVG(b.avg_positioning) AS last_28_days_value
			FROM positioning_basics a
			LEFT JOIN positioning_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3,4
								)

		SELECT *
		     , RANK() OVER (PARTITION BY order_date ORDER BY last_28_days_value DESC) AS last_28_days_rank
		FROM positioning_agg
	    WHERE last_28_days_value IS NOT NULL
	                                )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_28_days_value
         , (a.last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_28_days_score
    FROM positioning_28_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_28_days_rank) AS number_of_rows
        FROM positioning_28_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

eta_7 AS (
	WITH eta_7_pct_rank AS (
		WITH eta_agg AS (
		    WITH eta_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
				     , brand
					 , brand_type
					 , AVG(avg_eta) AS avg_eta
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_7_days_begin
				 , a.brand
				 , a.brand_type
			     , AVG(b.avg_eta) AS last_7_days_value
			FROM eta_basics a
			LEFT JOIN eta_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3,4
						)

		SELECT *
		     , RANK() OVER (PARTITION BY order_date ORDER BY last_7_days_value DESC) AS last_7_days_rank
		FROM eta_agg
		WHERE last_7_days_value IS NOT NULL
	                        )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_7_days_value
         , (a.last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_7_days_score
    FROM eta_7_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_7_days_rank) AS number_of_rows
        FROM eta_7_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

eta_28 AS (
	WITH eta_28_pct_rank AS (
		WITH eta_agg AS (
		    WITH eta_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
				     , brand
					 , brand_type
					 , AVG(avg_eta) AS avg_eta
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_28_days_begin
				 , a.brand
				 , a.brand_type
			     , AVG(b.avg_eta) AS last_28_days_value
			FROM eta_basics a
			LEFT JOIN eta_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3,4
						)

		SELECT *
		     , RANK() OVER (PARTITION BY order_date ORDER BY last_28_days_value DESC) AS last_28_days_rank
		FROM eta_agg
		WHERE last_28_days_value IS NOT NULL
	                        )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_28_days_value
         , (a.last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_28_days_score
    FROM eta_28_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_28_days_rank) AS number_of_rows
        FROM eta_28_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

number_of_storefronts_7 AS (
	WITH storefronts_7_pct_rank AS (
		WITH storefronts_agg AS (
		    WITH storefronts_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
				     , brand
					 , brand_type
					 , COUNT(DISTINCT unique_storefront_id) AS storefronts_sum
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								        )

			SELECT a.order_date
				 , a.last_7_days_begin
				 , a.brand
				 , a.brand_type
			     , AVG(b.storefronts_sum) AS last_7_days_value
			FROM storefronts_basics a
			LEFT JOIN storefronts_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3,4
								)

		SELECT *
		     , RANK() OVER (PARTITION BY order_date ORDER BY last_7_days_value) AS last_7_days_rank
		FROM storefronts_agg
		WHERE last_7_days_value IS NOT NULL
	                                )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_7_days_value
         , (a.last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_7_days_score
    FROM storefronts_7_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_7_days_rank) AS number_of_rows
        FROM storefronts_7_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

number_of_storefronts_28 AS (
	WITH storefronts_28_pct_rank AS (
		WITH storefronts_agg AS (
		    WITH storefronts_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
				     , brand
					 , brand_type
					 , COUNT(DISTINCT unique_storefront_id) AS storefronts_sum
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								        )

			SELECT a.order_date
				 , a.last_28_days_begin
				 , a.brand
				 , a.brand_type
			     , AVG(b.storefronts_sum) AS last_28_days_value
			FROM storefronts_basics a
			LEFT JOIN storefronts_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3,4
								)

		SELECT *
		     , RANK() OVER (PARTITION BY order_date ORDER BY last_28_days_value) AS last_28_days_rank
		FROM storefronts_agg
		WHERE last_28_days_value IS NOT NULL
	                                )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_28_days_value
         , (a.last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_28_days_score
    FROM storefronts_28_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_28_days_rank) AS number_of_rows
        FROM storefronts_28_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

sales_7 AS (
	WITH sales_7_pct_rank AS (
		WITH sales_agg AS (
		    WITH sales_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
				     , brand
					 , brand_type
					 , SUM(subtotal) AS sales
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_7_days_begin
				 , a.brand
				 , a.brand_type
			     , SUM(b.sales) AS last_7_days_value
			FROM sales_basics a
			LEFT JOIN sales_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3,4
						)

		SELECT *
		     , RANK() OVER (PARTITION BY order_date ORDER BY last_7_days_value) AS last_7_days_rank
		FROM sales_agg a
		WHERE last_7_days_value IS NOT NULL
	                            )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_7_days_value
         , (a.last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_7_days_score
    FROM sales_7_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_7_days_rank) AS number_of_rows
        FROM sales_7_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

sales_28 AS (
	WITH sales_28_pct_rank AS (
		WITH sales_agg AS (
		    WITH sales_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
				     , brand
					 , brand_type
					 , SUM(subtotal) AS sales
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_28_days_begin
				 , a.brand
				 , a.brand_type
			     , SUM(b.sales) AS last_28_days_value
			FROM sales_basics a
			LEFT JOIN sales_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3,4
						)

		SELECT *
		     , RANK() OVER (PARTITION BY order_date ORDER BY last_28_days_value) AS last_28_days_rank
		FROM sales_agg a
		WHERE last_28_days_value IS NOT NULL
	                            )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_28_days_value
         , (a.last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_28_days_score
    FROM sales_28_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_28_days_rank) AS number_of_rows
        FROM sales_28_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

marketing_7 AS (
	WITH marketing_7_pct_rank AS (
		WITH marketing_agg AS (
		    WITH marketing_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
				     , brand
					 , brand_type
					 , SUM(adjusted_marketing_spend) AS marketing_spend_sum
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								    )

			SELECT a.order_date
				 , a.last_7_days_begin
				 , a.brand
				 , a.brand_type
			     , SUM(b.marketing_spend_sum) AS last_7_days_value
			FROM marketing_basics a
			LEFT JOIN marketing_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3,4
							)

		SELECT *
		     , RANK() OVER (PARTITION BY order_date ORDER BY last_7_days_value) AS last_7_days_rank
		FROM marketing_agg
	    WHERE last_7_days_value IS NOT NULL
	                            )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_7_days_value
         , (a.last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_7_days_score
    FROM marketing_7_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_7_days_rank) AS number_of_rows
        FROM marketing_7_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

marketing_28 AS (
	WITH marketing_28_pct_rank AS (
		WITH marketing_agg AS (
		    WITH marketing_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
				     , brand
					 , brand_type
					 , SUM(adjusted_marketing_spend) AS marketing_spend_sum
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								    )

			SELECT a.order_date
				 , a.last_28_days_begin
				 , a.brand
				 , a.brand_type
			     , SUM(b.marketing_spend_sum) AS last_28_days_value
			FROM marketing_basics a
			LEFT JOIN marketing_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3,4
							)

		SELECT *
		     , RANK() OVER (PARTITION BY order_date ORDER BY last_28_days_value) AS last_28_days_rank
		FROM marketing_agg
	    WHERE last_28_days_value IS NOT NULL
	                            )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_28_days_value
         , (a.last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_28_days_score
    FROM marketing_28_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_28_days_rank) AS number_of_rows
        FROM marketing_28_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

sales_per_store_7 AS (
	WITH sps_7_pct_rank AS (
		WITH sps_agg AS (
		    WITH sps_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
				     , brand
					 , brand_type
					 , SUM(subtotal) AS sales
					 , COUNT(DISTINCT kitchen_code) AS kitchen_days
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_7_days_begin
				 , a.brand
				 , a.brand_type
			     , CASE
			         WHEN SUM(b.kitchen_days) = 0 THEN NULL
			         ELSE SUM(b.sales)/ SUM(b.kitchen_days) END AS last_7_days_value
			FROM sps_basics a
			LEFT JOIN sps_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3,4
				        )

		SELECT *
			 , RANK() OVER (PARTITION BY order_date ORDER BY last_7_days_value) AS last_7_days_rank
		FROM sps_agg
		WHERE last_7_days_value IS NOT NULL
	                        )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_7_days_value
         , (a.last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_7_days_score
    FROM sps_7_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_7_days_rank) AS number_of_rows
        FROM sps_7_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1
),

sales_per_store_28 AS (
	WITH sps_28_pct_rank AS (
		WITH sps_agg AS (
		    WITH sps_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
				     , brand
					 , brand_type
					 , SUM(subtotal) AS sales
					 , COUNT(DISTINCT kitchen_code) AS kitchen_days
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_28_days_begin
				 , a.brand
				 , a.brand_type
			     , CASE
			         WHEN SUM(b.kitchen_days) = 0 THEN NULL
			         ELSE SUM(b.sales)/ SUM(b.kitchen_days) END AS last_28_days_value
			FROM sps_basics a
			LEFT JOIN sps_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3,4
				        )

		SELECT *
			 , RANK() OVER (PARTITION BY order_date ORDER BY last_28_days_value) AS last_28_days_rank
		FROM sps_agg
		WHERE last_28_days_value IS NOT NULL
	                        )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_28_days_value
         , (a.last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_28_days_score
    FROM sps_28_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_28_days_rank) AS number_of_rows
        FROM sps_28_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

marketing_spend_per_store_7 AS (
	WITH mps_7_pct_rank AS (
		WITH mps_agg AS (
		    WITH mps_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
				     , brand
					 , brand_type
					 , SUM(adjusted_marketing_spend) AS marketing_spend_sum
					 , COUNT(DISTINCT kitchen_code) AS kitchen_days
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_7_days_begin
				 , a.brand
				 , a.brand_type
			     , CASE
			         WHEN SUM(b.kitchen_days) = 0 THEN NULL
			         ELSE SUM(b.marketing_spend_sum)/ SUM(b.kitchen_days) END AS last_7_days_value
			FROM mps_basics a
			LEFT JOIN mps_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3,4
				            )

		SELECT *
			 , RANK() OVER (PARTITION BY order_date ORDER BY last_7_days_value) AS last_7_days_rank
		FROM mps_agg
		WHERE last_7_days_value IS NOT NULL
	                )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_7_days_value
         , (a.last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_7_days_score
    FROM mps_7_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_7_days_rank) AS number_of_rows
        FROM mps_7_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

marketing_spend_per_store_28 AS (
	WITH mps_28_pct_rank AS (
		WITH mps_agg AS (
		    WITH mps_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
				     , brand
					 , brand_type
					 , SUM(adjusted_marketing_spend) AS marketing_spend_sum
					 , COUNT(DISTINCT kitchen_code) AS kitchen_days
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_28_days_begin
				 , a.brand
				 , a.brand_type
			     , CASE
			         WHEN SUM(b.kitchen_days) = 0 THEN NULL
			         ELSE SUM(b.marketing_spend_sum)/ SUM(b.kitchen_days) END AS last_28_days_value
			FROM mps_basics a
			LEFT JOIN mps_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3,4
				            )

		SELECT *
			 , RANK() OVER (PARTITION BY order_date ORDER BY last_28_days_value) AS last_28_days_rank
		FROM mps_agg
		WHERE last_28_days_value IS NOT NULL
	                )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_28_days_value
         , (a.last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_28_days_score
    FROM mps_28_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_28_days_rank) AS number_of_rows
        FROM mps_28_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

roi_7 AS (
	WITH roi_7_pct_rank AS (
		WITH roi_agg AS (
		    WITH roi_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
				     , brand
					 , brand_type
				     , SUM(subtotal) AS sales
					 , SUM(adjusted_marketing_spend) AS marketing_spend_sum
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_7_days_begin
				 , a.brand
				 , a.brand_type
			     , CASE
			         WHEN SUM(b.marketing_spend_sum) = 0 THEN NULL
			         ELSE SUM(b.sales)/ SUM(b.marketing_spend_sum) END AS last_7_days_value
			FROM roi_basics a
			LEFT JOIN roi_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3,4
					 )

		SELECT *
			 , RANK() OVER (PARTITION BY order_date ORDER BY last_7_days_value) AS last_7_days_rank
		FROM roi_agg
		WHERE last_7_days_value IS NOT NULL
	                    )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_7_days_value
         , (a.last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_7_days_score
    FROM roi_7_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_7_days_rank) AS number_of_rows
        FROM roi_7_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

roi_28 AS (
	WITH roi_28_pct_rank AS (
		WITH roi_agg AS (
		    WITH roi_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
				     , brand
					 , brand_type
				     , SUM(subtotal) AS sales
					 , SUM(adjusted_marketing_spend) AS marketing_spend_sum
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_28_days_begin
				 , a.brand
				 , a.brand_type
			     , CASE
			         WHEN SUM(b.marketing_spend_sum) = 0 THEN NULL
			         ELSE SUM(b.sales)/ SUM(b.marketing_spend_sum) END AS last_28_days_value
			FROM roi_basics a
			LEFT JOIN roi_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3,4
					 )

		SELECT *
			 , RANK() OVER (PARTITION BY order_date ORDER BY last_28_days_value) AS last_28_days_rank
		FROM roi_agg
		WHERE last_28_days_value IS NOT NULL
	                    )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_28_days_value
         , (a.last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_28_days_score
    FROM roi_28_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_28_days_rank) AS number_of_rows
        FROM roi_28_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

roas_7 AS (
	WITH roas_7_pct_rank AS (
		WITH roas_agg AS (
		    WITH roas_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
				     , brand
					 , brand_type
				     , SUM(redemptions_dollars) AS promo_sales
					 , SUM(adjusted_marketing_spend) AS marketing_spend_sum
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_7_days_begin
				 , a.brand
				 , a.brand_type
			     , CASE
			         WHEN SUM(b.marketing_spend_sum) = 0 THEN NULL
			         ELSE SUM(b.promo_sales)/ SUM(b.marketing_spend_sum) END AS last_7_days_value
			FROM roas_basics a
			LEFT JOIN roas_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3,4
					 )

		SELECT *
			 , RANK() OVER (PARTITION BY order_date ORDER BY last_7_days_value) AS last_7_days_rank
		FROM roas_agg
		WHERE last_7_days_value IS NOT NULL
	                    )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_7_days_value
         , (a.last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_7_days_score
    FROM roas_7_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_7_days_rank) AS number_of_rows
        FROM roas_7_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

roas_28 AS (
	WITH roas_28_pct_rank AS (
		WITH roas_agg AS (
		    WITH roas_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
				     , brand
					 , brand_type
				     , SUM(redemptions_dollars) AS promo_sales
					 , SUM(adjusted_marketing_spend) AS marketing_spend_sum
				FROM brand_optimisation.daily_storefront_performance
			    GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_28_days_begin
				 , a.brand
				 , a.brand_type
			     , CASE
			         WHEN SUM(b.marketing_spend_sum) = 0 THEN NULL
			         ELSE SUM(b.promo_sales)/ SUM(b.marketing_spend_sum) END AS last_28_days_value
			FROM roas_basics a
			LEFT JOIN roas_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3,4
					 )

		SELECT *
			 , RANK() OVER (PARTITION BY order_date ORDER BY last_28_days_value) AS last_28_days_rank
		FROM roas_agg
		WHERE last_28_days_value IS NOT NULL
	                    )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_28_days_value
         , (a.last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_28_days_score
    FROM roas_28_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_28_days_rank) AS number_of_rows
        FROM roas_28_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

conversion_rate_7 AS (
	WITH conv_7_pct_rank AS (
		WITH conv_agg AS (
		    WITH conv_basics AS (
				SELECT a.order_date
					 , DATEADD('day', -6, a.order_date) ::date AS last_7_days_begin
				     , a.brand
					 , a.brand_type
				     , SUM(b.viewed_menu) AS storefront_views
					 , SUM(b.placed_an_order) AS placed_an_order_sum
				FROM brand_optimisation.daily_storefront_performance a
				LEFT JOIN uber_eats_scraper.customers b ON a.order_date = b.platform_date AND a.shop_id = b.restaurant_id
				GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_7_days_begin
				 , a.brand
				 , a.brand_type
			     , CASE
			         WHEN SUM(b.storefront_views) = 0 THEN NULL
			         ELSE SUM(b.placed_an_order_sum)/ SUM(b.storefront_views) ::float END AS last_7_days_value
			FROM conv_basics a
			LEFT JOIN conv_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3,4
					 )

		SELECT *
			 , RANK() OVER (PARTITION BY order_date ORDER BY last_7_days_value) AS last_7_days_rank
		FROM conv_agg
		WHERE last_7_days_value IS NOT NULL
	                        )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_7_days_value
         , (a.last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_7_days_score
    FROM conv_7_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_7_days_rank) AS number_of_rows
        FROM conv_7_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

conversion_rate_28 AS (
	WITH conv_28_pct_rank AS (
		WITH conv_agg AS (
		    WITH conv_basics AS (
				SELECT a.order_date
					 , DATEADD('day', -27, a.order_date) ::date AS last_28_days_begin
				     , a.brand
					 , a.brand_type
				     , SUM(b.viewed_menu) AS storefront_views
					 , SUM(b.placed_an_order) AS placed_an_order_sum
				FROM brand_optimisation.daily_storefront_performance a
				LEFT JOIN uber_eats_scraper.customers b ON a.order_date = b.platform_date AND a.shop_id = b.restaurant_id
				GROUP BY 1,3,4
								 )

			SELECT a.order_date
				 , a.last_28_days_begin
				 , a.brand
				 , a.brand_type
			     , CASE
			         WHEN SUM(b.storefront_views) = 0 THEN NULL
			         ELSE SUM(b.placed_an_order_sum)/ SUM(b.storefront_views) ::float END AS last_28_days_value
			FROM conv_basics a
			LEFT JOIN conv_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3,4
					 )

		SELECT *
			 , RANK() OVER (PARTITION BY order_date ORDER BY last_28_days_value) AS last_28_days_rank
		FROM conv_agg
		WHERE last_28_days_value IS NOT NULL
	                        )

    SELECT a.order_date
         , a.brand
         , a.brand_type
         , a.last_28_days_value
         , (a.last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS last_28_days_score
    FROM conv_28_pct_rank a
    LEFT JOIN (
        SELECT order_date, MAX(last_28_days_rank) AS number_of_rows
        FROM conv_28_pct_rank
        GROUP BY 1
	    ) b ON a.order_date = b.order_date
	WHERE b.number_of_rows > 1

),

operational_score AS (

    SELECT * FROM (
	    SELECT DISTINCT dsp.order_date
	         , dsp.brand
	         , dsp.brand_type
	         , CASE
		        WHEN ( (a.last_7_days_score IS NULL) OR (b.last_7_days_score IS NULL) OR (c.last_7_days_score IS NULL) ) THEN NULL
		        ELSE (0.25 * a.last_7_days_score + 0.25 * b.last_7_days_score + 0.5 * c.last_7_days_score) END ::float AS last_7_days_score
	         , CASE
		        WHEN ( (d.last_28_days_score IS NULL) OR (e.last_28_days_score IS NULL) OR (f.last_28_days_score IS NULL) ) THEN NULL
		        ELSE (0.25 * d.last_28_days_score + 0.25 * e.last_28_days_score + 0.5 * f.last_28_days_score) END ::float AS last_28_days_score
		FROM brand_optimisation.daily_storefront_performance dsp
	    LEFT JOIN uptime_7 a ON dsp.order_date = a.order_date AND dsp.brand = a.brand
	    LEFT JOIN defect_rate_7 b ON dsp.order_date = b.order_date AND dsp.brand = b.brand
	    LEFT JOIN eta_7 c ON dsp.order_date = c.order_date AND dsp.brand = c.brand
	    LEFT JOIN uptime_28 d ON dsp.order_date = d.order_date AND dsp.brand = d.brand
	    LEFT JOIN defect_rate_28 e ON dsp.order_date = e.order_date AND dsp.brand = e.brand
	    LEFT JOIN eta_28 f ON dsp.order_date = f.order_date AND dsp.brand = f.brand
				)
    WHERE last_7_days_score IS NOT NULL
      AND last_28_days_score IS NOT NULL
),

brand_benchmark AS (
	WITH reference_dates AS (
		SELECT DISTINCT order_date
	    FROM brand_optimisation.daily_storefront_performance
	                        )
	SELECT a.order_date
	     , '/*Benchmark: All Brands Median*/' AS brand
	     , NULL AS brand_type
		 , b.metric
	     , NULL AS last_7_days_value
	     , NULL AS last_28_days_value
		 , b.last_7_days_score
		 , b.last_28_days_score
	FROM reference_dates a
	CROSS JOIN scratch.doris_brand_benchmark b
					)


SELECT COALESCE(a.order_date, b.order_date) AS order_date
     , COALESCE(a.brand, b.brand) AS brand
     , COALESCE(a.brand_type, b.brand_type) AS brand_type
     , 'Uptime' AS metric
     , a.last_7_days_value
     , b.last_28_days_value
	 , a.last_7_days_score
	 , b.last_28_days_score
FROM uptime_7 a
FULL OUTER JOIN (
    SELECT * FROM uptime_28
	) b ON a.order_date = b.order_date AND a.brand = b.brand

UNION

SELECT COALESCE(a.order_date, b.order_date) AS order_date
     , COALESCE(a.brand, b.brand) AS brand
     , COALESCE(a.brand_type, b.brand_type) AS brand_type
     , 'Defect Rate' AS metric
     , a.last_7_days_value
     , b.last_28_days_value
	 , a.last_7_days_score
	 , b.last_28_days_score
FROM defect_rate_7 a
FULL OUTER JOIN (
    SELECT * FROM defect_rate_28
	) b ON a.order_date = b.order_date AND a.brand = b.brand

UNION

SELECT COALESCE(a.order_date, b.order_date) AS order_date
     , COALESCE(a.brand, b.brand) AS brand
     , COALESCE(a.brand_type, b.brand_type) AS brand_type
     , 'Rating' AS metric
     , a.last_7_days_value
     , b.last_28_days_value
	 , a.last_7_days_score
	 , b.last_28_days_score
FROM rating_7 a
FULL OUTER JOIN (
    SELECT * FROM rating_28
	) b ON a.order_date = b.order_date AND a.brand = b.brand

UNION

SELECT COALESCE(a.order_date, b.order_date) AS order_date
     , COALESCE(a.brand, b.brand) AS brand
     , COALESCE(a.brand_type, b.brand_type) AS brand_type
     , 'Positioning' AS metric
     , a.last_7_days_value
     , b.last_28_days_value
	 , a.last_7_days_score
	 , b.last_28_days_score
FROM Positioning_7 a
FULL OUTER JOIN (
    SELECT * FROM Positioning_28
	) b ON a.order_date = b.order_date AND a.brand = b.brand

UNION

SELECT COALESCE(a.order_date, b.order_date) AS order_date
     , COALESCE(a.brand, b.brand) AS brand
     , COALESCE(a.brand_type, b.brand_type) AS brand_type
     , 'ETA' AS metric
     , a.last_7_days_value
     , b.last_28_days_value
	 , a.last_7_days_score
	 , b.last_28_days_score
FROM eta_7 a
FULL OUTER JOIN (
    SELECT * FROM eta_28
	) b ON a.order_date = b.order_date AND a.brand = b.brand

UNION

SELECT COALESCE(a.order_date, b.order_date) AS order_date
     , COALESCE(a.brand, b.brand) AS brand
     , COALESCE(a.brand_type, b.brand_type) AS brand_type
     , 'Number of Storefronts' AS metric
     , a.last_7_days_value
     , b.last_28_days_value
	 , a.last_7_days_score
	 , b.last_28_days_score
FROM number_of_storefronts_7 a
FULL OUTER JOIN (
    SELECT * FROM number_of_storefronts_28
	) b ON a.order_date = b.order_date AND a.brand = b.brand

UNION

SELECT COALESCE(a.order_date, b.order_date) AS order_date
     , COALESCE(a.brand, b.brand) AS brand
     , COALESCE(a.brand_type, b.brand_type) AS brand_type
     , 'Sales' AS metric
     , a.last_7_days_value
     , b.last_28_days_value
	 , a.last_7_days_score
	 , b.last_28_days_score
FROM sales_7 a
FULL OUTER JOIN (
    SELECT * FROM sales_28
	) b ON a.order_date = b.order_date AND a.brand = b.brand

UNION

SELECT COALESCE(a.order_date, b.order_date) AS order_date
     , COALESCE(a.brand, b.brand) AS brand
     , COALESCE(a.brand_type, b.brand_type) AS brand_type
     , 'Marketing Spend' AS metric
     , a.last_7_days_value
     , b.last_28_days_value
	 , a.last_7_days_score
	 , b.last_28_days_score
FROM marketing_7 a
FULL OUTER JOIN (
    SELECT * FROM marketing_28
	) b ON a.order_date = b.order_date AND a.brand = b.brand

UNION

SELECT COALESCE(a.order_date, b.order_date) AS order_date
     , COALESCE(a.brand, b.brand) AS brand
     , COALESCE(a.brand_type, b.brand_type) AS brand_type
     , 'Sales Per Store' AS metric
     , a.last_7_days_value
     , b.last_28_days_value
	 , a.last_7_days_score
	 , b.last_28_days_score
FROM sales_per_store_7 a
FULL OUTER JOIN (
    SELECT * FROM sales_per_store_28
	) b ON a.order_date = b.order_date AND a.brand = b.brand

UNION

SELECT COALESCE(a.order_date, b.order_date) AS order_date
     , COALESCE(a.brand, b.brand) AS brand
     , COALESCE(a.brand_type, b.brand_type) AS brand_type
     , 'Marketing Spend Per Store' AS metric
     , a.last_7_days_value
     , b.last_28_days_value
	 , a.last_7_days_score
	 , b.last_28_days_score
FROM marketing_spend_per_store_7 a
FULL OUTER JOIN (
    SELECT * FROM marketing_spend_per_store_28
	) b ON a.order_date = b.order_date AND a.brand = b.brand

UNION

SELECT COALESCE(a.order_date, b.order_date) AS order_date
     , COALESCE(a.brand, b.brand) AS brand
     , COALESCE(a.brand_type, b.brand_type) AS brand_type
     , 'ROI' AS metric
     , a.last_7_days_value
     , b.last_28_days_value
	 , a.last_7_days_score
	 , b.last_28_days_score
FROM roi_7 a
FULL OUTER JOIN (
    SELECT * FROM roi_28
	) b ON a.order_date = b.order_date AND a.brand = b.brand

UNION

SELECT COALESCE(a.order_date, b.order_date) AS order_date
     , COALESCE(a.brand, b.brand) AS brand
     , COALESCE(a.brand_type, b.brand_type) AS brand_type
     , 'ROAS' AS metric
     , a.last_7_days_value
     , b.last_28_days_value
	 , a.last_7_days_score
	 , b.last_28_days_score
FROM roas_7 a
FULL OUTER JOIN (
    SELECT * FROM roas_28
	) b ON a.order_date = b.order_date AND a.brand = b.brand

UNION

SELECT COALESCE(a.order_date, b.order_date) AS order_date
     , COALESCE(a.brand, b.brand) AS brand
     , COALESCE(a.brand_type, b.brand_type) AS brand_type
     , 'Conversion Rate' AS metric
     , a.last_7_days_value
     , b.last_28_days_value
	 , a.last_7_days_score
	 , b.last_28_days_score
FROM conversion_rate_7 a
FULL OUTER JOIN (
    SELECT * FROM conversion_rate_28
	) b ON a.order_date = b.order_date AND a.brand = b.brand

UNION

SELECT order_date
	 , brand
	 , brand_type
	 , 'Composite Operational Score' AS metric
	 , NULL AS last_7_days_value
	 , NULL AS last_28_days_value
	 , last_7_days_score
	 , last_28_days_score
FROM operational_score

UNION

SELECT * FROM brand_benchmark

;







