WITH scorecard_kitchen AS (
    WITH uptime_7 AS (
		WITH uptime_7_pct_rank AS (
			WITH uptime_7_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
					 , brand
					 , kitchen_code
					 , SUM(downtime) AS downtime_sum
					 , SUM(mins_open) AS mins_open_sum
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									 )

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY uptime_store_last_7_days) AS uptime_store_last_7_days_rank
			FROM (
			    SELECT a.order_date
					 , a.brand
					 , a.kitchen_code
					 , CASE
						WHEN SUM(b.mins_open_sum) = 0 THEN NULL
					    ELSE ( 1 - SUM(b.downtime_sum)/ SUM(b.mins_open_sum) ) END ::FLOAT AS uptime_store_last_7_days
				FROM uptime_7_basics a
				LEFT JOIN uptime_7_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
			WHERE uptime_store_last_7_days IS NOT NULL
									)

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.uptime_store_last_7_days
			 , (a.uptime_store_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS uptime_store_last_7_days_score
		FROM uptime_7_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(uptime_store_last_7_days_rank) AS number_of_rows
			FROM uptime_7_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    uptime_28 AS (
		WITH uptime_28_pct_rank AS (
			WITH uptime_28_basics AS (
				SELECT order_date
					, DATEADD('day', -27, order_date) ::date AS last_28_days_begin
					, brand
					, kitchen_code
					, SUM(downtime) AS downtime_sum
					, SUM(mins_open) AS mins_open_sum
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									 )

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY uptime_store_last_28_days) AS uptime_store_last_28_days_rank
			FROM (
				SELECT a.order_date
					 , a.brand
					 , a.kitchen_code
					 , CASE
						WHEN SUM(b.mins_open_sum) = 0 THEN NULL
					    ELSE ( 1 - SUM(b.downtime_sum)/ SUM(b.mins_open_sum) ) END ::FLOAT AS uptime_store_last_28_days
				FROM uptime_28_basics a
				LEFT JOIN uptime_28_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
			WHERE uptime_store_last_28_days IS NOT NULL
								)

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.uptime_store_last_28_days
			 , (a.uptime_store_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS uptime_store_last_28_days_score
		FROM uptime_28_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(uptime_store_last_28_days_rank) AS number_of_rows
			FROM uptime_28_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1
    ),

    defect_7 AS (
		WITH defect_7_pct_rank AS (
			WITH defect_7_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
					 , brand
					 , kitchen_code
					 , SUM(refunds) AS refunds_sum
					 , SUM(cancelled_gmv) AS cancelled_gmv_sum
					 , SUM(unfulfilled_gmv) AS unfulfilled_gmv_sum
					 , SUM(subtotal) AS subtotal_sum
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									 )

			SELECT *
				 , PERCENT_RANK() OVER (PARTITION BY order_date ORDER BY defect_store_last_7_days DESC) ::FLOAT * 5 AS defect_store_last_7_days_rank
			FROM (
				SELECT a.order_date
					 , a.brand
					 , a.kitchen_code
					 , CASE
						WHEN SUM(b.subtotal_sum) = 0 THEN NULL
					    ELSE (sum(b.refunds_sum) + sum(b.cancelled_gmv_sum) + sum(b.unfulfilled_gmv_sum))/sum(b.subtotal_sum) END ::FLOAT AS defect_store_last_7_days
				FROM defect_7_basics a
				LEFT JOIN defect_7_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
			WHERE defect_store_last_7_days IS NOT NULL
									)

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.defect_store_last_7_days
			 , (a.defect_store_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS defect_store_last_7_days_score
		FROM defect_7_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(defect_store_last_7_days_rank) AS number_of_rows
			FROM defect_7_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

	defect_28 AS (
		WITH defect_28_pct_rank AS (
			WITH defect_28_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
					 , brand
					 , kitchen_code
					 , SUM(refunds) AS refunds_sum
					 , SUM(cancelled_gmv) AS cancelled_gmv_sum
					 , SUM(unfulfilled_gmv) AS unfulfilled_gmv_sum
					 , SUM(subtotal) AS subtotal_sum
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									 )

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY defect_store_last_28_days DESC) AS defect_store_last_28_days_rank
			FROM (
				SELECT a.order_date
				     , a.brand
				     , a.kitchen_code
				     , CASE
						WHEN SUM(b.subtotal_sum) = 0 THEN NULL
				        ELSE (sum(b.refunds_sum) + sum(b.cancelled_gmv_sum) + sum(b.unfulfilled_gmv_sum))/sum(b.subtotal_sum) END ::FLOAT AS defect_store_last_28_days
				FROM defect_28_basics a
				LEFT JOIN defect_28_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
		    WHERE defect_store_last_28_days IS NOT NULL
		                            )

		SELECT a.order_date
		     , a.brand
		     , a.kitchen_code
		     , a.defect_store_last_28_days
		     , (a.defect_store_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS defect_store_last_28_days_score
		FROM defect_28_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(defect_store_last_28_days_rank) AS number_of_rows
			FROM defect_28_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
	    WHERE b.number_of_rows > 1

	),

    rating_7 AS (
		WITH rating_7_pct_rank AS (
			WITH rating_7_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
					 , brand
					 , kitchen_code
					 , AVG(last_rating) AS avg_rating
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									 )

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY rating_store_last_7_days) AS rating_store_last_7_days_rank
			FROM (
				SELECT a.order_date
					 , a.brand
					 , a.kitchen_code
				     , AVG(b.avg_rating) ::float AS rating_store_last_7_days
				FROM rating_7_basics a
				LEFT JOIN rating_7_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
			WHERE rating_store_last_7_days IS NOT NULL
		                            )

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.rating_store_last_7_days
			 , (a.rating_store_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS rating_store_last_7_days_score
		FROM rating_7_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(rating_store_last_7_days_rank) AS number_of_rows
			FROM rating_7_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    rating_28 AS (
		WITH rating_28_pct_rank AS (
			WITH rating_28_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
					 , brand
					 , kitchen_code
					 , AVG(last_rating) AS avg_rating
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1, 3, 4
			)

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY rating_store_last_28_days) AS rating_store_last_28_days_rank
			FROM (
				     SELECT a.order_date
					      , a.brand
					      , a.kitchen_code
					      , AVG(b.avg_rating) ::float AS rating_store_last_28_days
				     FROM rating_28_basics a
				          LEFT JOIN rating_28_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND
				                                          b.order_date BETWEEN a.last_28_days_begin AND a.order_date
				     GROUP BY 1, 2, 3
			     )
			WHERE rating_store_last_28_days IS NOT NULL
								)

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.rating_store_last_28_days
			 , (a.rating_store_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS rating_store_last_28_days_score
		FROM rating_28_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(rating_store_last_28_days_rank) AS number_of_rows
			FROM rating_28_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    positioning_7 AS (
		WITH positioning_7_pct_rank AS (
			WITH positioning_7_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
					 , brand
					 , kitchen_code
					 , AVG(avg_page_ranking) AS avg_positioning
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									 )

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY positioning_store_last_7_days DESC) AS positioning_store_last_7_days_rank
			FROM (
				SELECT a.order_date
				     , a.brand
					 , a.kitchen_code
					 , AVG(b.avg_positioning) ::float AS positioning_store_last_7_days
				FROM positioning_7_basics a
				LEFT JOIN positioning_7_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
			WHERE positioning_store_last_7_days IS NOT NULL
		                                )

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.positioning_store_last_7_days
			 , (a.positioning_store_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS positioning_store_last_7_days_score
		FROM positioning_7_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(positioning_store_last_7_days_rank) AS number_of_rows
			FROM positioning_7_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    positioning_28 AS (
		WITH positioning_28_pct_rank AS (
			WITH positioning_28_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
					 , brand
					 , kitchen_code
					 , AVG(avg_page_ranking) AS avg_positioning
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									 )

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY positioning_store_last_28_days DESC) AS positioning_store_last_28_days_rank
			FROM (
				SELECT a.order_date
					 , a.brand
				     , a.kitchen_code
				     , AVG(b.avg_positioning) ::float AS positioning_store_last_28_days
				FROM positioning_28_basics a
				LEFT JOIN positioning_28_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
			WHERE positioning_store_last_28_days IS NOT NULL
		                                )

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.positioning_store_last_28_days
			 , (a.positioning_store_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS positioning_store_last_28_days_score
		FROM positioning_28_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(positioning_store_last_28_days_rank) AS number_of_rows
			FROM positioning_28_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    eta_7 AS (
		WITH eta_7_pct_rank AS (
			WITH eta_7_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
					 , brand
					 , kitchen_code
					 , AVG(avg_eta) AS avg_eta
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									 )

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY eta_store_last_7_days DESC) AS eta_store_last_7_days_rank
			FROM (
			    SELECT a.order_date
				     , a.brand
					 , a.kitchen_code
					 , AVG(b.avg_eta) ::float AS eta_store_last_7_days
				FROM eta_7_basics a
				LEFT JOIN eta_7_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
		    WHERE eta_store_last_7_days IS NOT NULL
		                        )

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.eta_store_last_7_days
			 , (a.eta_store_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS eta_store_last_7_days_score
		FROM eta_7_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(eta_store_last_7_days_rank) AS number_of_rows
			FROM eta_7_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    eta_28 AS (
		WITH eta_28_pct_rank AS (
			WITH eta_28_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
					 , brand
					 , kitchen_code
					 , AVG(avg_eta) AS avg_eta
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									 )

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY eta_store_last_28_days DESC) AS eta_store_last_28_days_rank
			FROM (
				SELECT a.order_date
				     , a.brand
				     , a.kitchen_code
				     , AVG(b.avg_eta) ::float AS eta_store_last_28_days
				FROM eta_28_basics a
				LEFT JOIN eta_28_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
			WHERE eta_store_last_28_days IS NOT NULL
		                        )

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.eta_store_last_28_days
			 , (a.eta_store_last_28_days_rank - 1)/ (b.number_of_rows - 1) AS eta_store_last_28_days_score
		FROM eta_28_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(eta_store_last_28_days_rank) AS number_of_rows
			FROM eta_28_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    storefronts_7 AS (
		WITH storefronts_7_pct_rank AS (
			WITH storefronts_7_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
					 , brand
					 , kitchen_code
					 , COUNT(DISTINCT unique_storefront_id) AS storefronts_sum
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									 )

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY storefronts_store_last_7_days) AS storefronts_store_last_7_days_rank
			FROM (
				SELECT a.order_date
				     , a.brand
				     , a.kitchen_code
				     , AVG(b.storefronts_sum) ::float AS storefronts_store_last_7_days
				FROM storefronts_7_basics a
				LEFT JOIN storefronts_7_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
			WHERE storefronts_store_last_7_days IS NOT NULL
		                            )

		SELECT a.order_date
				 , a.brand
				 , a.kitchen_code
				 , a.storefronts_store_last_7_days
				 , (a.storefronts_store_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS storefronts_store_last_7_days_score
			FROM storefronts_7_pct_rank a
			LEFT JOIN (
				SELECT order_date, MAX(storefronts_store_last_7_days_rank) AS number_of_rows
				FROM storefronts_7_pct_rank
				GROUP BY 1
				) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    storefronts_28 AS (
		WITH storefronts_28_pct_rank AS (
			WITH storefronts_28_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
					 , brand
					 , kitchen_code
					 , COUNT(DISTINCT unique_storefront_id) AS storefronts_sum
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									 )

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY storefronts_store_last_28_days) AS storefronts_store_last_28_days_rank
			FROM (
				SELECT a.order_date
					 , a.brand
				     , a.kitchen_code
					 , AVG(b.storefronts_sum) ::float AS storefronts_store_last_28_days
				FROM storefronts_28_basics a
				LEFT JOIN storefronts_28_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
			WHERE storefronts_store_last_28_days IS NOT NULL
		                            )

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.storefronts_store_last_28_days
			 , (a.storefronts_store_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS storefronts_store_last_28_days_score
		FROM storefronts_28_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(storefronts_store_last_28_days_rank) AS number_of_rows
			FROM storefronts_28_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    marketing_7 AS (
		WITH marketing_7_pct_rank AS (
			WITH marketing_7_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
					 , brand
					 , kitchen_code
					 , SUM(adjusted_marketing_spend) AS marketing_spend_sum
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1, 3, 4
			)

			SELECT *
				 , marketing_spend_store_last_7_days/ 7 ::float AS avg_marketing_spend_store_last_7_days
				 , RANK() OVER (PARTITION BY order_date ORDER BY avg_marketing_spend_store_last_7_days) AS avg_marketing_spend_store_last_7_days_rank
			FROM (
				     SELECT a.order_date
					      , a.brand
					      , a.kitchen_code
					      , SUM(b.marketing_spend_sum) ::float AS marketing_spend_store_last_7_days
				     FROM marketing_7_basics a
				     LEFT JOIN marketing_7_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
				     GROUP BY 1, 2, 3
			     )
			WHERE marketing_spend_store_last_7_days IS NOT NULL
									)

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
		     , a.avg_marketing_spend_store_last_7_days
			 , (a.avg_marketing_spend_store_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS avg_marketing_spend_store_last_7_days_score
		FROM marketing_7_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(avg_marketing_spend_store_last_7_days_rank) AS number_of_rows
			FROM marketing_7_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    marketing_28 AS (
		WITH marketing_28_pct_rank AS (
			WITH marketing_28_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
					 , brand
					 , kitchen_code
					 , SUM(adjusted_marketing_spend) AS marketing_spend_sum
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1, 3, 4
										)

			SELECT *
			     , marketing_spend_store_last_28_days/ 28 ::float AS avg_marketing_spend_store_last_28_days
				 , RANK() OVER (PARTITION BY order_date ORDER BY avg_marketing_spend_store_last_28_days) AS avg_marketing_spend_store_last_28_days_rank
			FROM (
				SELECT a.order_date
					 , a.brand
					 , a.kitchen_code
					 , SUM(b.marketing_spend_sum) ::float AS marketing_spend_store_last_28_days
				FROM marketing_28_basics a
				LEFT JOIN marketing_28_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
			WHERE marketing_spend_store_last_28_days IS NOT NULL
		                            )

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
		     , a.avg_marketing_spend_store_last_28_days
			 , (a.avg_marketing_spend_store_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS avg_marketing_spend_store_last_28_days_score
		FROM marketing_28_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(avg_marketing_spend_store_last_28_days_rank) AS number_of_rows
			FROM marketing_28_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    sales_7 AS (
		WITH sales_7_pct_rank AS (
			WITH sales_7_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
					 , brand
					 , kitchen_code
					 , SUM(subtotal) AS sales_sum
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									 )

			SELECT *
			     , sales_store_last_7_days/ operational_days_store_last_7_days AS avg_sales_store_last_7_days
				 , RANK() OVER (PARTITION BY order_date ORDER BY avg_sales_store_last_7_days) AS avg_sales_store_last_7_days_rank
			FROM (
				SELECT a.order_date
					 , a.brand
					 , a.kitchen_code
					 , SUM(b.sales_sum) ::float AS sales_store_last_7_days
					 , COUNT(DISTINCT b.order_date) AS operational_days_store_last_7_days
				FROM sales_7_basics a
				LEFT JOIN sales_7_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
			WHERE sales_store_last_7_days IS NOT NULL AND operational_days_store_last_7_days > 0
								)

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.operational_days_store_last_7_days
		     , a.avg_sales_store_last_7_days
			 , (a.avg_sales_store_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS avg_sales_store_last_7_days_score
		FROM sales_7_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(avg_sales_store_last_7_days_rank) AS number_of_rows
			FROM sales_7_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    sales_28 AS (
		WITH sales_28_pct_rank AS (
			WITH sales_28_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
					 , brand
					 , kitchen_code
					 , SUM(subtotal) AS sales_sum
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									)
			SELECT *
			     , sales_store_last_28_days/ operational_days_store_last_28_days AS avg_sales_store_last_28_days
				 , RANK() OVER (PARTITION BY order_date ORDER BY avg_sales_store_last_28_days) AS avg_sales_store_last_28_days_rank
			FROM (
				SELECT a.order_date
					 , a.brand
					 , a.kitchen_code
					 , SUM(b.sales_sum) ::float AS sales_store_last_28_days
					 , COUNT(DISTINCT b.order_date) AS operational_days_store_last_28_days
				FROM sales_28_basics a
				LEFT JOIN sales_28_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
		    WHERE sales_store_last_28_days IS NOT NULL AND operational_days_store_last_28_days > 0
		                        )

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.operational_days_store_last_28_days
		     , a.avg_sales_store_last_28_days
			 , (a.avg_sales_store_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS avg_sales_store_last_28_days_score
		FROM sales_28_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(avg_sales_store_last_28_days_rank) AS number_of_rows
			FROM sales_28_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    roi_7 AS (
		WITH roi_7_pct_rank AS (
			WITH roi_7_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
					 , brand
					 , kitchen_code
					 , SUM(subtotal) AS sales
					 , SUM(adjusted_marketing_spend) AS marketing_spend
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									 )

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY roi_store_last_7_days) AS roi_store_last_7_days_rank
			FROM (
				SELECT a.order_date
				     , a.brand
				     , a.kitchen_code
				     , CASE
				         WHEN SUM(b.marketing_spend) = 0 THEN NULL
				         ELSE SUM(b.sales)/ SUM(b.marketing_spend) END AS roi_store_last_7_days
				FROM roi_7_basics a
				LEFT JOIN roi_7_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
			WHERE roi_store_last_7_days IS NOT NULL
		                        )

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.roi_store_last_7_days
			 , (a.roi_store_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS roi_store_last_7_days_score
		FROM roi_7_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(roi_store_last_7_days_rank) AS number_of_rows
			FROM roi_7_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    roi_28 AS (
		WITH roi_28_pct_rank AS (
			WITH roi_28_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
					 , brand
					 , kitchen_code
					 , SUM(subtotal) AS sales
					 , SUM(adjusted_marketing_spend) AS marketing_spend
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									)

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY roi_store_last_28_days) AS roi_store_last_28_days_rank
			FROM (
				SELECT a.order_date
					 , a.brand
					 , a.kitchen_code
				     , CASE
				         WHEN SUM(b.marketing_spend) = 0 THEN NULL
				         ELSE SUM(b.sales)/ SUM(b.marketing_spend) END AS roi_store_last_28_days
				FROM roi_28_basics a
				LEFT JOIN roi_28_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
		    WHERE roi_store_last_28_days IS NOT NULL
							)

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.roi_store_last_28_days
			 , (a.roi_store_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS roi_store_last_28_days_score
		FROM roi_28_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(roi_store_last_28_days_rank) AS number_of_rows
			FROM roi_28_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    roas_7 AS (
		WITH roas_7_pct_rank AS (
			WITH roas_7_basics AS (
				SELECT order_date
					 , DATEADD('day', -6, order_date) ::date AS last_7_days_begin
					 , brand
					 , kitchen_code
					 , SUM(redemptions_dollars) AS promo_sales
					 , SUM(adjusted_marketing_spend) AS marketing_spend
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									 )

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY roas_store_last_7_days) AS roas_store_last_7_days_rank
			FROM (
				SELECT a.order_date
				     , a.brand
				     , a.kitchen_code
				     , CASE
				         WHEN SUM(b.marketing_spend) = 0 THEN NULL
				         ELSE SUM(b.promo_sales)/ SUM(b.marketing_spend) END AS roas_store_last_7_days
				FROM roas_7_basics a
				LEFT JOIN roas_7_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
			WHERE roas_store_last_7_days IS NOT NULL
		                        )

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.roas_store_last_7_days
			 , (a.roas_store_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS roas_store_last_7_days_score
		FROM roas_7_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(roas_store_last_7_days_rank) AS number_of_rows
			FROM roas_7_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    roas_28 AS (
		WITH roas_28_pct_rank AS (
			WITH roas_28_basics AS (
				SELECT order_date
					 , DATEADD('day', -27, order_date) ::date AS last_28_days_begin
					 , brand
					 , kitchen_code
					 , SUM(redemptions_dollars) AS promo_sales
					 , SUM(adjusted_marketing_spend) AS marketing_spend
				FROM brand_optimisation.daily_storefront_performance
				GROUP BY 1,3,4
									)

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY roas_store_last_28_days) AS roas_store_last_28_days_rank
			FROM (
				SELECT a.order_date
					 , a.brand
					 , a.kitchen_code
				     , CASE
				         WHEN SUM(b.marketing_spend) = 0 THEN NULL
				         ELSE SUM(b.promo_sales)/ SUM(b.marketing_spend) END AS roas_store_last_28_days
				FROM roas_28_basics a
				LEFT JOIN roas_28_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
		    WHERE roas_store_last_28_days IS NOT NULL
							)

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.roas_store_last_28_days
			 , (a.roas_store_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS roas_store_last_28_days_score
		FROM roas_28_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(roas_store_last_28_days_rank) AS number_of_rows
			FROM roas_28_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    conv_7 AS (
		WITH conv_7_pct_rank AS (
			WITH conv_7_basics AS (
				SELECT a.order_date
					 , DATEADD('day', -6, a.order_date) ::date AS last_7_days_begin
					 , a.brand
					 , a.kitchen_code
					 , SUM(b.viewed_menu) AS storefront_views
					 , SUM(b.placed_an_order) AS placed_an_order_sum
				FROM brand_optimisation.daily_storefront_performance a
				LEFT JOIN uber_eats_scraper.customers b ON a.order_date = b.platform_date AND a.shop_id = b.restaurant_id
				GROUP BY 1,3,4
									 )

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY conversion_store_last_7_days) AS conversion_store_last_7_days_rank
			FROM (
				SELECT a.order_date
				     , a.brand
				     , a.kitchen_code
				     , AVG(b.storefront_views) AS storefront_views_avg_last_7_days
				     , CASE
				         WHEN SUM(b.storefront_views) = 0 THEN NULL
				         ELSE SUM(b.placed_an_order_sum)/ SUM(b.storefront_views) ::float END AS conversion_store_last_7_days
				FROM conv_7_basics a
				LEFT JOIN conv_7_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
			WHERE conversion_store_last_7_days IS NOT NULL
		                        )

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.conversion_store_last_7_days
		     , a.storefront_views_avg_last_7_days
			 , (a.conversion_store_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS conversion_store_last_7_days_score
		FROM conv_7_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(conversion_store_last_7_days_rank) AS number_of_rows
			FROM conv_7_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

    conv_28 AS (
		WITH conv_28_pct_rank AS (
			WITH conv_28_basics AS (
				SELECT a.order_date
					 , DATEADD('day', -27, a.order_date) ::date AS last_28_days_begin
					 , a.brand
					 , a.kitchen_code
					 , SUM(b.viewed_menu) AS storefront_views
					 , SUM(b.placed_an_order) AS placed_an_order_sum
				FROM brand_optimisation.daily_storefront_performance a
				LEFT JOIN uber_eats_scraper.customers b ON a.order_date = b.platform_date AND a.shop_id = b.restaurant_id
				GROUP BY 1,3,4
									)

			SELECT *
				 , RANK() OVER (PARTITION BY order_date ORDER BY conversion_store_last_28_days) AS conversion_store_last_28_days_rank
			FROM (
				SELECT a.order_date
					 , a.brand
					 , a.kitchen_code
				     , AVG(b.storefront_views) AS storefront_views_avg_last_28_days
				     , CASE
				         WHEN SUM(b.storefront_views) = 0 THEN NULL
				         ELSE SUM(b.placed_an_order_sum)/ SUM(b.storefront_views) ::float END AS conversion_store_last_28_days
				FROM conv_28_basics a
				LEFT JOIN conv_28_basics b ON a.brand = b.brand AND a.kitchen_code = b.kitchen_code AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
				GROUP BY 1,2,3
				     )
		    WHERE conversion_store_last_28_days IS NOT NULL
		                        )

		SELECT a.order_date
			 , a.brand
			 , a.kitchen_code
			 , a.conversion_store_last_28_days
		     , a.storefront_views_avg_last_28_days
			 , (a.conversion_store_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS conversion_store_last_28_days_score
		FROM conv_28_pct_rank a
		LEFT JOIN (
			SELECT order_date, MAX(conversion_store_last_28_days_rank) AS number_of_rows
			FROM conv_28_pct_rank
			GROUP BY 1
			) b ON a.order_date = b.order_date
        WHERE b.number_of_rows > 1

	),

	dsp AS (
	    SELECT DISTINCT order_date
	         , brand_type
	         , brand
	         , kitchen_code
	    FROM brand_optimisation.daily_storefront_performance
	),

    store_age AS (
        SELECT brand
             , kitchen_code
             , MIN(order_date) AS first_order_date
        FROM brand_optimisation.daily_storefront_performance
        GROUP BY 1,2
    )


	SELECT dsp.order_date
		 , dsp.brand
	     , dsp.brand_type
		 , dsp.kitchen_code
	     , store_age.first_order_date
	     , a.uptime_store_last_7_days
		 , a.uptime_store_last_7_days_score
	     , b.uptime_store_last_28_days
		 , b.uptime_store_last_28_days_score
	     , c.defect_store_last_7_days
		 , c.defect_store_last_7_days_score
	     , d.defect_store_last_28_days
		 , d.defect_store_last_28_days_score
	     , e.rating_store_last_7_days
		 , e.rating_store_last_7_days_score
	     , f.rating_store_last_28_days
		 , f.rating_store_last_28_days_score
	     , g.positioning_store_last_7_days
		 , g.positioning_store_last_7_days_score
	     , h.positioning_store_last_28_days
		 , h.positioning_store_last_28_days_score
	     , i.eta_store_last_7_days
		 , i.eta_store_last_7_days_score
	     , j.eta_store_last_28_days
		 , j.eta_store_last_28_days_score
	     , k.storefronts_store_last_7_days
		 , k.storefronts_store_last_7_days_score
	     , l.storefronts_store_last_28_days
		 , l.storefronts_store_last_28_days_score
	     , m.avg_marketing_spend_store_last_7_days
		 , m.avg_marketing_spend_store_last_7_days_score
	     , n.avg_marketing_spend_store_last_28_days
		 , n.avg_marketing_spend_store_last_28_days_score
		 , o.operational_days_store_last_7_days
	     , o.avg_sales_store_last_7_days
		 , o.avg_sales_store_last_7_days_score
	     , p.operational_days_store_last_28_days
	     , p.avg_sales_store_last_28_days
		 , p.avg_sales_store_last_28_days_score
		 , q.roi_store_last_7_days
	     , q.roi_store_last_7_days_score
	     , r.roi_store_last_28_days
		 , r.roi_store_last_28_days_score
	     , s.storefront_views_avg_last_7_days
		 , s.conversion_store_last_7_days
	     , s.conversion_store_last_7_days_score
	     , t.storefront_views_avg_last_28_days
	     , t.conversion_store_last_28_days
		 , t.conversion_store_last_28_days_score
    	 , u.roas_store_last_7_days
	     , u.roas_store_last_7_days_score
	     , v.roas_store_last_28_days
		 , v.roas_store_last_28_days_score
	FROM dsp
	LEFT JOIN store_age ON dsp.brand = store_age.brand AND dsp.kitchen_code = store_age.kitchen_code
	LEFT JOIN uptime_7 a ON dsp.order_date = a.order_date AND dsp.brand = a.brand AND dsp.kitchen_code = a.kitchen_code
	LEFT JOIN uptime_28 b ON dsp.order_date = b.order_date AND dsp.brand = b.brand AND dsp.kitchen_code = b.kitchen_code
	LEFT JOIN defect_7 c ON dsp.order_date = c.order_date AND dsp.brand = c.brand AND dsp.kitchen_code = c.kitchen_code
	LEFT JOIN defect_28 d ON dsp.order_date = d.order_date AND dsp.brand = d.brand AND dsp.kitchen_code = d.kitchen_code
	LEFT JOIN rating_7 e ON dsp.order_date = e.order_date AND dsp.brand = e.brand AND dsp.kitchen_code = e.kitchen_code
	LEFT JOIN rating_28 f ON dsp.order_date = f.order_date AND dsp.brand = f.brand AND dsp.kitchen_code = f.kitchen_code
	LEFT JOIN positioning_7 g ON dsp.order_date = g.order_date AND dsp.brand = g.brand AND dsp.kitchen_code = g.kitchen_code
	LEFT JOIN positioning_28 h ON dsp.order_date = h.order_date AND dsp.brand = h.brand AND dsp.kitchen_code = h.kitchen_code
	LEFT JOIN eta_7 i ON dsp.order_date = i.order_date AND dsp.brand = i.brand AND dsp.kitchen_code = i.kitchen_code
	LEFT JOIN eta_28 j ON dsp.order_date = j.order_date AND dsp.brand = j.brand AND dsp.kitchen_code = j.kitchen_code
	LEFT JOIN storefronts_7 k ON dsp.order_date = k.order_date AND dsp.brand = k.brand AND dsp.kitchen_code = k.kitchen_code
	LEFT JOIN storefronts_28 l ON dsp.order_date = l.order_date AND dsp.brand = l.brand AND dsp.kitchen_code = l.kitchen_code
	LEFT JOIN marketing_7 m ON dsp.order_date = m.order_date AND dsp.brand = m.brand AND dsp.kitchen_code = m.kitchen_code
	LEFT JOIN marketing_28 n ON dsp.order_date = n.order_date AND dsp.brand = n.brand AND dsp.kitchen_code = n.kitchen_code
	LEFT JOIN sales_7 o ON dsp.order_date = o.order_date AND dsp.brand = o.brand AND dsp.kitchen_code = o.kitchen_code
	LEFT JOIN sales_28 p ON dsp.order_date = p.order_date AND dsp.brand = p.brand AND dsp.kitchen_code = p.kitchen_code
	LEFT JOIN roi_7 q ON dsp.order_date = q.order_date AND dsp.brand = q.brand AND dsp.kitchen_code = q.kitchen_code
	LEFT JOIN roi_28 r ON dsp.order_date = r.order_date AND dsp.brand = r.brand AND dsp.kitchen_code = r.kitchen_code
	LEFT JOIN conv_7 s ON dsp.order_date = s.order_date AND dsp.brand = s.brand AND dsp.kitchen_code = s.kitchen_code
	LEFT JOIN conv_28 t ON dsp.order_date = t.order_date AND dsp.brand = t.brand AND dsp.kitchen_code = t.kitchen_code
    LEFT JOIN roas_7 u ON dsp.order_date = u.order_date AND dsp.brand = u.brand AND dsp.kitchen_code = u.kitchen_code
	LEFT JOIN roas_28 v ON dsp.order_date = v.order_date AND dsp.brand = v.brand AND dsp.kitchen_code = v.kitchen_code

)

SELECT s.*
	 , CASE
	     WHEN ( (s.uptime_store_last_7_days_score IS NULL) OR (s.defect_store_last_7_days_score IS NULL) OR (s.eta_store_last_7_days_score IS NULL) ) THEN NULL
         ELSE (0.25 * s.uptime_store_last_7_days_score + 0.25 * s.defect_store_last_7_days_score + 0.5 * s.eta_store_last_7_days_score) END AS operational_score_7
	 , CASE
	     WHEN ( (s.uptime_store_last_28_days_score IS NULL) OR (s.defect_store_last_28_days_score IS NULL) OR (s.eta_store_last_28_days_score IS NULL) ) THEN NULL
         ELSE (0.25 * s.uptime_store_last_28_days_score + 0.25 * s.defect_store_last_28_days_score + 0.5 * s.eta_store_last_28_days_score ) END AS operational_score_28
FROM scorecard_kitchen s



;
