WITH uptime_7 AS (
	WITH uptime_7_pct_rank AS (
		WITH uptime_7_basics AS (
			SELECT order_date
				, DATEADD('day', -6, order_date) ::date AS last_7_days_begin
				, brand
				, brand_type
				, SUM(downtime) AS downtime_sum
				, SUM(mins_open) AS mins_open_sum
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
								)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY uptime_last_7_days) AS uptime_last_7_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
				, CASE
				    WHEN SUM(b.mins_open_sum) = 0 THEN NULL
				    ELSE ( 1 - SUM(b.downtime_sum)/ SUM(b.mins_open_sum) ) END ::float AS uptime_last_7_days
			FROM uptime_7_basics a
			LEFT JOIN uptime_7_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
		    GROUP BY 1,2,3
			)
		WHERE uptime_last_7_days IS NOT NULL
							)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.uptime_last_7_days
		, (a.uptime_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS uptime_last_7_days_score
	FROM uptime_7_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(uptime_last_7_days_rank) AS number_of_rows
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
				, brand_type
				, SUM(downtime) AS downtime_sum
				, SUM(mins_open) AS mins_open_sum
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
								)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY uptime_last_28_days) AS uptime_last_28_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
				, CASE
					WHEN SUM(b.mins_open_sum) = 0 THEN NULL
					ELSE ( 1 - SUM(b.downtime_sum)/ SUM(b.mins_open_sum) ) END ::float AS uptime_last_28_days
			FROM uptime_28_basics a
			LEFT JOIN uptime_28_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE uptime_last_28_days IS NOT NULL
				)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.uptime_last_28_days
		, (a.uptime_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS uptime_last_28_days_score
	FROM uptime_28_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(uptime_last_28_days_rank) AS number_of_rows
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
				, brand_type
				, SUM(refunds) AS refunds_sum
				, SUM(cancelled_gmv) AS cancelled_gmv_sum
				, SUM(unfulfilled_gmv) AS unfulfilled_gmv_sum
				, SUM(subtotal) AS subtotal_sum
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
								)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY defect_last_7_days DESC) AS defect_last_7_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
				, CASE
				    WHEN SUM(b.subtotal_sum) = 0 THEN NULL
				    ELSE (sum(b.refunds_sum) + sum(b.cancelled_gmv_sum) + sum(b.unfulfilled_gmv_sum))/sum(b.subtotal_sum) END ::float AS defect_last_7_days
		FROM defect_7_basics a
		LEFT JOIN defect_7_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
		GROUP BY 1,2,3
				)
		WHERE defect_last_7_days IS NOT NULL
						)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.defect_last_7_days
		, (a.defect_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS defect_last_7_days_score
	FROM defect_7_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(defect_last_7_days_rank) AS number_of_rows
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
				, brand_type
				, SUM(refunds) AS refunds_sum
				, SUM(cancelled_gmv) AS cancelled_gmv_sum
				, SUM(unfulfilled_gmv) AS unfulfilled_gmv_sum
				, SUM(subtotal) AS subtotal_sum
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
								)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY defect_last_28_days DESC) AS defect_last_28_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
				, CASE
					WHEN SUM(b.subtotal_sum) = 0 THEN NULL
				    ELSE (sum(b.refunds_sum) + sum(b.cancelled_gmv_sum) + sum(b.unfulfilled_gmv_sum))/sum(b.subtotal_sum) END ::float AS defect_last_28_days
			FROM defect_28_basics a
			LEFT JOIN defect_28_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE defect_last_28_days IS NOT NULL
							)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.defect_last_28_days
		, (a.defect_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS defect_last_28_days_score
	FROM defect_28_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(defect_last_28_days_rank) AS number_of_rows
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
				, brand_type
				, AVG(last_rating) AS avg_rating
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
								)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY rating_last_7_days) AS rating_last_7_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
				, AVG(b.avg_rating) ::float AS rating_last_7_days
			FROM rating_7_basics a
			LEFT JOIN rating_7_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE rating_last_7_days IS NOT NULL
							)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.rating_last_7_days
		, (a.rating_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS rating_last_7_days_score
	FROM rating_7_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(rating_last_7_days_rank) AS number_of_rows
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
				, brand_type
				, AVG(last_rating) AS avg_rating
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
								)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY rating_last_28_days) AS rating_last_28_days_rank
		FROM (
			 SELECT a.order_date
				, a.brand
				, a.brand_type
				, AVG(b.avg_rating) ::float AS rating_last_28_days
			FROM rating_28_basics a
			LEFT JOIN rating_28_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE rating_last_28_days IS NOT NULL
								)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.rating_last_28_days
		, (a.rating_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS rating_last_28_days_score
	FROM rating_28_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(rating_last_28_days_rank) AS number_of_rows
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
				, brand_type
				, AVG(avg_page_ranking) AS avg_positioning
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
									 )

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY positioning_last_7_days DESC) AS positioning_last_7_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
				, AVG(b.avg_positioning) ::float AS positioning_last_7_days
		FROM positioning_7_basics a
		LEFT JOIN positioning_7_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
		GROUP BY 1,2,3
			)
		WHERE positioning_last_7_days IS NOT NULL
									)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.positioning_last_7_days
		, (a.positioning_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS positioning_last_7_days_score
	FROM positioning_7_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(positioning_last_7_days_rank) AS number_of_rows
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
				, brand_type
				, AVG(avg_page_ranking) ::float AS avg_positioning
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
									 )

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY positioning_last_28_days DESC) AS positioning_last_28_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
				, AVG(b.avg_positioning) ::float AS positioning_last_28_days
			FROM positioning_28_basics a
			LEFT JOIN positioning_28_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE positioning_last_28_days IS NOT NULL
									)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.positioning_last_28_days
		, (a.positioning_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS positioning_last_28_days_score
	FROM positioning_28_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(positioning_last_28_days_rank) AS number_of_rows
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
				, brand_type
				, AVG(avg_eta) AS avg_eta
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
							)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY eta_last_7_days DESC) AS eta_last_7_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
				, AVG(b.avg_eta) ::FLOAT AS eta_last_7_days
			FROM eta_7_basics a
			LEFT JOIN eta_7_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE eta_last_7_days > 0
		                )

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.eta_last_7_days
		, (a.eta_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS eta_last_7_days_score
	FROM eta_7_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(eta_last_7_days_rank) AS number_of_rows
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
				, brand_type
				, AVG(avg_eta) AS avg_eta
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
							)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY eta_last_28_days DESC) AS eta_last_28_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
				, AVG(b.avg_eta) ::float AS eta_last_28_days
			FROM eta_28_basics a
			LEFT JOIN eta_28_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE eta_last_28_days IS NOT NULL
							)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.eta_last_28_days
		, (a.eta_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS eta_last_28_days_score
	FROM eta_28_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(eta_last_28_days_rank) AS number_of_rows
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
				, brand_type
				, COUNT(DISTINCT unique_storefront_id) AS storefronts_sum
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
									 )

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY storefronts_last_7_days) AS storefronts_last_7_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
				, AVG(b.storefronts_sum) ::float AS storefronts_last_7_days
			FROM storefronts_7_basics a
			LEFT JOIN storefronts_7_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE storefronts_last_7_days IS NOT NULL
								)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.storefronts_last_7_days
		, (a.storefronts_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS storefronts_last_7_days_score
	FROM storefronts_7_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(storefronts_last_7_days_rank) AS number_of_rows
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
				, brand_type
				, COUNT(DISTINCT unique_storefront_id) AS storefronts_sum
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
									 )

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY storefronts_last_28_days) AS storefronts_last_28_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
				, AVG(b.storefronts_sum) ::float AS storefronts_last_28_days
			FROM storefronts_28_basics a
			LEFT JOIN storefronts_28_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE storefronts_last_28_days IS NOT NULL
									)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.storefronts_last_28_days
		, (a.storefronts_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS storefronts_last_28_days_score
	FROM storefronts_28_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(storefronts_last_28_days_rank) AS number_of_rows
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
				, brand_type
				, SUM(adjusted_marketing_spend) AS marketing_spend_sum
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
									)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY avg_marketing_spend_last_7_days) AS avg_marketing_spend_last_7_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
				, SUM(b.marketing_spend_sum)/ 7 ::float AS avg_marketing_spend_last_7_days
			FROM marketing_7_basics a
			LEFT JOIN marketing_7_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE avg_marketing_spend_last_7_days IS NOT NULL
								)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.avg_marketing_spend_last_7_days
		, (a.avg_marketing_spend_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS avg_marketing_spend_last_7_days_score
	FROM marketing_7_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(avg_marketing_spend_last_7_days_rank) AS number_of_rows
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
				, brand_type
				, SUM(adjusted_marketing_spend) AS marketing_spend_sum
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1, 3, 4
									)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY avg_marketing_spend_last_28_days) AS avg_marketing_spend_last_28_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
				, SUM(b.marketing_spend_sum)/ 28 ::float AS avg_marketing_spend_last_28_days
			FROM marketing_28_basics a
			LEFT JOIN marketing_28_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE avg_marketing_spend_last_28_days IS NOT NULL
									)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.avg_marketing_spend_last_28_days
		, (a.avg_marketing_spend_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS avg_marketing_spend_last_28_days_score
	FROM marketing_28_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(avg_marketing_spend_last_28_days_rank) AS number_of_rows
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
				, brand_type
				, SUM(subtotal) AS sales_sum
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
								)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY avg_sales_last_7_days) AS avg_sales_last_7_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
			    , COUNT(DISTINCT b.order_date) AS operational_days_last_7_days
				, SUM(b.sales_sum)/ COUNT(DISTINCT b.order_date) ::float AS avg_sales_last_7_days
			FROM sales_7_basics a
			LEFT JOIN sales_7_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE avg_sales_last_7_days IS NOT NULL
							)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.operational_days_last_7_days
		, a.avg_sales_last_7_days
		, (a.avg_sales_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS avg_sales_last_7_days_score
	FROM sales_7_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(avg_sales_last_7_days_rank) AS number_of_rows
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
				, brand_type
				, SUM(subtotal) AS sales_sum
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
								)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY avg_sales_last_28_days) AS avg_sales_last_28_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
			    , COUNT(DISTINCT b.order_date) AS operational_days_last_28_days
				, SUM(b.sales_sum)/ COUNT(DISTINCT b.order_date)  ::float AS avg_sales_last_28_days
			FROM sales_28_basics a
			LEFT JOIN sales_28_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE avg_sales_last_28_days IS NOT NULL
							)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.operational_days_last_28_days
		, a.avg_sales_last_28_days
		, (a.avg_sales_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS avg_sales_last_28_days_score
	FROM sales_28_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(avg_sales_last_28_days_rank) AS number_of_rows
		FROM sales_28_pct_rank
		GROUP BY 1
		) b ON a.order_date = b.order_date
    WHERE b.number_of_rows > 1
),

sales_per_store_7 AS (
	WITH sales_per_store_7_pct_rank AS (
		WITH sps_7_basics AS (
			SELECT order_date
				, DATEADD('day', -6, order_date) ::date AS last_7_days_begin
				, brand
				, brand_type
				, SUM(subtotal) AS sales
				, COUNT(DISTINCT kitchen_code) AS kitchen_days
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
							)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY avg_sps_last_7_days) AS avg_sps_last_7_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
			    , CASE
			        WHEN SUM(b.kitchen_days) = 0 THEN NULL
			        ELSE SUM(b.sales)/ SUM(b.kitchen_days) END ::float AS avg_sps_last_7_days
			FROM sps_7_basics a
			LEFT JOIN sps_7_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE avg_sps_last_7_days IS NOT NULL
										)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.avg_sps_last_7_days
		, (a.avg_sps_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS avg_sps_last_7_days_score
	FROM sales_per_store_7_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(avg_sps_last_7_days_rank) AS number_of_rows
		FROM sales_per_store_7_pct_rank
		GROUP BY 1
		) b ON a.order_date = b.order_date
    WHERE b.number_of_rows > 1
),

sales_per_store_28 AS (
	WITH sales_per_store_28_pct_rank AS (
		WITH sps_28_basics AS (
			SELECT order_date
				, DATEADD('day', -27, order_date) ::date AS last_28_days_begin
				, brand
				, brand_type
				, SUM(subtotal) AS sales
				, COUNT(DISTINCT kitchen_code) AS kitchen_days
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
							)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY avg_sps_last_28_days) AS avg_sps_last_28_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
			    , CASE
			        WHEN SUM(b.kitchen_days) = 0 THEN NULL
			        ELSE SUM(b.sales)/ SUM(b.kitchen_days) END ::float AS avg_sps_last_28_days
			FROM sps_28_basics a
			LEFT JOIN sps_28_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE avg_sps_last_28_days IS NOT NULL
										)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.avg_sps_last_28_days
		, (a.avg_sps_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS avg_sps_last_28_days_score
	FROM sales_per_store_28_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(avg_sps_last_28_days_rank) AS number_of_rows
		FROM sales_per_store_28_pct_rank
		GROUP BY 1
		) b ON a.order_date = b.order_date
    WHERE b.number_of_rows > 1
),

marketing_per_store_7 AS (
	WITH marketing_per_store_7_pct_rank AS (
		WITH mps_7_basics AS (
			SELECT order_date
				, DATEADD('day', -6, order_date) ::date AS last_7_days_begin
				, brand
				, brand_type
				, SUM(adjusted_marketing_spend) AS marketing_spend_sum
				, COUNT(DISTINCT kitchen_code) AS stores
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
							 )

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY avg_mps_last_7_days) AS avg_mps_last_7_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
			    , CASE
			        WHEN AVG(b.stores) = 0 THEN NULL
			        ELSE ( SUM(b.marketing_spend_sum)/ AVG(b.stores) )/ 7 END ::float AS avg_mps_last_7_days
			FROM mps_7_basics a
			LEFT JOIN mps_7_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE avg_mps_last_7_days IS NOT NULL
										)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.avg_mps_last_7_days
		, (a.avg_mps_last_7_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS avg_mps_last_7_days_score
	FROM marketing_per_store_7_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(avg_mps_last_7_days_rank) AS number_of_rows
		FROM marketing_per_store_7_pct_rank
		GROUP BY 1
		) b ON a.order_date = b.order_date
    WHERE b.number_of_rows > 1
),

marketing_per_store_28 AS (
	WITH marketing_per_store_28_pct_rank AS (
		WITH mps_28_basics AS (
			SELECT order_date
				, DATEADD('day', -27, order_date) ::date AS last_28_days_begin
				, brand
				, brand_type
				, SUM(adjusted_marketing_spend) AS marketing_spend_sum
				, COUNT(DISTINCT kitchen_code) AS stores
			FROM brand_optimisation.daily_storefront_performance
			GROUP BY 1,3,4
							)

		SELECT *
			, RANK() OVER (PARTITION BY order_date ORDER BY avg_mps_last_28_days) AS avg_mps_last_28_days_rank
		FROM (
			SELECT a.order_date
				, a.brand
				, a.brand_type
			    , CASE
			        WHEN AVG(b.stores) = 0 THEN NULL
			        ELSE ( SUM(b.marketing_spend_sum)/ AVG(b.stores) )/ 28 END ::FLOAT AS avg_mps_last_28_days
			FROM mps_28_basics a
			LEFT JOIN mps_28_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
			GROUP BY 1,2,3
			)
		WHERE avg_mps_last_28_days IS NOT NULL
		                                )

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, a.avg_mps_last_28_days
		, (a.avg_mps_last_28_days_rank - 1)/ (b.number_of_rows - 1) ::float * 5 AS avg_mps_last_28_days_score
	FROM marketing_per_store_28_pct_rank a
	LEFT JOIN (
		SELECT order_date, MAX(avg_mps_last_28_days_rank) AS number_of_rows
		FROM marketing_per_store_28_pct_rank
		GROUP BY 1
		) b ON a.order_date = b.order_date
    WHERE b.number_of_rows > 1
),

orders_per_store_7 AS (
	WITH ops_7_basics AS (
		SELECT order_date
			, DATEADD('day', -6, order_date) ::date AS last_7_days_begin
			, brand
			, brand_type
			, SUM(orders) AS orders
			, COUNT(DISTINCT kitchen_code) AS kitchen_days
		FROM brand_optimisation.daily_storefront_performance
		GROUP BY 1,3,4
						)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, CASE
			WHEN SUM(b.kitchen_days) = 0 THEN NULL
			ELSE SUM(b.orders)/ SUM(b.kitchen_days) END ::float AS avg_ops_last_7_days
		, CASE
			WHEN SUM(b.kitchen_days) = 0 THEN NULL
			ELSE SUM(b.orders)/ COUNT(DISTINCT b.order_date) END ::float AS avg_orders_last_7_days
	FROM ops_7_basics a
	LEFT JOIN ops_7_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
	GROUP BY 1,2,3
),

orders_per_store_28 AS (
	WITH ops_28_basics AS (
		SELECT order_date
			, DATEADD('day', -27, order_date) ::date AS last_28_days_begin
			, brand
			, brand_type
			, SUM(orders) AS orders
			, COUNT(DISTINCT kitchen_code) AS kitchen_days
		FROM brand_optimisation.daily_storefront_performance
		GROUP BY 1,3,4
							)

	SELECT a.order_date
		, a.brand
		, a.brand_type
		, CASE
			WHEN SUM(b.kitchen_days) = 0 THEN NULL
			ELSE SUM(b.orders)/ SUM(b.kitchen_days) END ::float AS avg_ops_last_28_days
		, CASE
			WHEN SUM(b.kitchen_days) = 0 THEN NULL
			ELSE SUM(b.orders)/ COUNT(DISTINCT b.order_date) END ::float AS avg_orders_last_28_days
	FROM ops_28_basics a
	LEFT JOIN ops_28_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
	GROUP BY 1,2,3
),

roi_7 AS (
	WITH roi_7_basics AS (
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
		, a.brand
		, a.brand_type
		, CASE
			WHEN SUM(b.marketing_spend_sum) = 0 THEN NULL
			ELSE SUM(b.sales)/ SUM(b.marketing_spend_sum) END ::float AS roi_last_7_days
	FROM roi_7_basics a
	LEFT JOIN roi_7_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_7_days_begin AND a.order_date
	GROUP BY 1,2,3
),

roi_28 AS (
	WITH roi_28_basics AS (
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
		, a.brand
		, a.brand_type
		, CASE
			WHEN SUM(b.marketing_spend_sum) = 0 THEN NULL
			ELSE SUM(b.sales)/ SUM(b.marketing_spend_sum) END ::float AS roi_last_28_days
	FROM roi_28_basics a
	LEFT JOIN roi_28_basics b ON a.brand = b.brand AND b.order_date BETWEEN a.last_28_days_begin AND a.order_date
	GROUP BY 1,2,3
),

dsp AS (
    SELECT a.order_date
         , a.brand
         , a.brand_type
         , SUM(a.subtotal) AS sales
         , PERCENT_RANK() OVER (PARTITION BY a.order_date ORDER BY sales) ::float * 5 AS sales_score
         , COUNT(DISTINCT a.kitchen_code) AS stores
         , SUM(a.subtotal)/ stores AS sales_per_store
         , PERCENT_RANK() OVER (PARTITION BY a.order_date ORDER BY sales_per_store) ::float * 5 AS sps_score
         , SUM(a.orders) AS orders
         , SUM(a.orders)/ stores AS orders_per_store
         , AVG(a.retention_84d) AS retention_rate
         , COUNT(DISTINCT a.unique_storefront_id) AS number_of_storefronts
         , SUM(a.downtime) AS downtime
         , SUM(a.mins_open) AS mins_open
         , SUM(a.refunds) AS refunds
         , SUM(a.cancelled_gmv) AS cancelled_gmv
         , SUM(a.unfulfilled_gmv) AS unfulfilled_gmv
         , AVG(a.last_rating) AS rating
         , AVG(a.avg_page_ranking) AS positioning
         , AVG(a.avg_eta) AS eta
         , SUM(b.viewed_menu) AS storefront_views
         , SUM(b.placed_an_order) AS placed_an_order
    FROM brand_optimisation.daily_storefront_performance a
    LEFT JOIN uber_eats_scraper.customers b ON a.order_date = b.platform_date AND a.shop_id = b.restaurant_id
    GROUP BY 1,2,3
),

dsp_marketing AS (
    SELECT order_date
         , brand
         , brand_type
         , COUNT(DISTINCT kitchen_code) AS stores
         , SUM(redemptions_dollars) AS promo_sales
         , SUM(redemptions) AS promo_orders
         , SUM(adjusted_marketing_spend) AS marketing_spend_sum
         , PERCENT_RANK() OVER (PARTITION BY order_date ORDER BY marketing_spend_sum) ::float * 5 AS marketing_spend_score
         , SUM(adjusted_marketing_spend)/ stores AS marketing_per_store
         , PERCENT_RANK() OVER (PARTITION BY order_date ORDER BY marketing_per_store) ::float * 5 AS mps_score
    FROM brand_optimisation.daily_storefront_performance
    WHERE adjusted_marketing_spend > 0 AND delivery_partner <> 'skipthedishes' AND lower(delivery_partner) <> 'olo'
	GROUP BY 1,2,3
    order by 1 desc
)

SELECT dsp.order_date
     , dsp.brand
     , dsp.brand_type
     , dsp.sales
     , dsp.sales_score
     , dsp.stores
     , dsp.sales_per_store
     , dsp.sps_score
     , dsp.orders
     , dsp.orders_per_store
     , dsp.retention_rate
     , dsp.number_of_storefronts
     , dsp.downtime
     , dsp.mins_open
     , dsp.refunds
     , dsp.cancelled_gmv
     , dsp.unfulfilled_gmv
     , dsp.rating
     , dsp.positioning
     , dsp.eta
     , dsp.storefront_views
     , dsp.placed_an_order
     , dsp_marketing.promo_sales
     , dsp_marketing.promo_orders
     , dsp_marketing.marketing_spend_sum
     , dsp_marketing.marketing_spend_score
     , dsp_marketing.marketing_per_store
     , dsp_marketing.mps_score
	 , a.uptime_last_7_days
	 , a.uptime_last_7_days_score
	 , b.uptime_last_28_days
	 , b.uptime_last_28_days_score
	 , c.defect_last_7_days
	 , c.defect_last_7_days_score
	 , d.defect_last_28_days
	 , d.defect_last_28_days_score
	 , e.rating_last_7_days
	 , e.rating_last_7_days_score
	 , f.rating_last_28_days
	 , f.rating_last_28_days_score
	 , g.positioning_last_7_days
	 , g.positioning_last_7_days_score
	 , h.positioning_last_28_days
	 , h.positioning_last_28_days_score
	 , i.eta_last_7_days
	 , i.eta_last_7_days_score
	 , j.eta_last_28_days
	 , j.eta_last_28_days_score
	 , k.storefronts_last_7_days
	 , k.storefronts_last_7_days_score
	 , l.storefronts_last_28_days
	 , l.storefronts_last_28_days_score
	 , m.avg_marketing_spend_last_7_days
	 , m.avg_marketing_spend_last_7_days_score
	 , n.avg_marketing_spend_last_28_days
	 , n.avg_marketing_spend_last_28_days_score
	 , o.operational_days_last_7_days
	 , o.avg_sales_last_7_days
	 , o.avg_sales_last_7_days_score
	 , p.operational_days_last_28_days
	 , p.avg_sales_last_28_days
	 , p.avg_sales_last_28_days_score
	 , q.avg_sps_last_7_days
	 , q.avg_sps_last_7_days_score
	 , r.avg_sps_last_28_days
	 , r.avg_sps_last_28_days_score
	 , s.avg_mps_last_7_days
	 , s.avg_mps_last_7_days_score
	 , t.avg_mps_last_28_days
	 , t.avg_mps_last_28_days_score
     , u.avg_orders_last_7_days
     , u.avg_ops_last_7_days
     , v.avg_orders_last_28_days
     , v.avg_ops_last_28_days
     , w.roi_last_7_days
     , x.roi_last_28_days
	 , CASE
	     WHEN ( (a.uptime_last_7_days_score IS NULL) OR (c.defect_last_7_days_score IS NULL) OR (i.eta_last_7_days_score IS NULL) ) THEN NULL
	     ELSE (0.25 * a.uptime_last_7_days_score + 0.25 * c.defect_last_7_days_score + 0.5 * i.eta_last_7_days_score) END AS operational_score_7
	 , CASE
	     WHEN ( (b.uptime_last_28_days_score IS NULL) OR (d.defect_last_28_days_score IS NULL) OR (j.eta_last_28_days_score IS NULL) ) THEN NULL
         ELSE (0.25 * b.uptime_last_28_days_score + 0.25 * d.defect_last_28_days_score + 0.5 * j.eta_last_28_days_score) END AS operational_score_28
FROM dsp
LEFT JOIN dsp_marketing ON dsp.order_date = dsp_marketing.order_date AND dsp.brand = dsp_marketing.brand
LEFT JOIN uptime_7 a ON dsp.order_date = a.order_date AND dsp.brand = a.brand
LEFT JOIN uptime_28 b ON dsp.order_date = b.order_date AND dsp.brand = b.brand
LEFT JOIN defect_7 c ON dsp.order_date = c.order_date AND dsp.brand = c.brand
LEFT JOIN defect_28 d ON dsp.order_date = d.order_date AND dsp.brand = d.brand
LEFT JOIN rating_7 e ON dsp.order_date = e.order_date AND dsp.brand = e.brand
LEFT JOIN rating_28 f ON dsp.order_date = f.order_date AND dsp.brand = f.brand
LEFT JOIN positioning_7 g ON dsp.order_date = g.order_date AND dsp.brand = g.brand
LEFT JOIN positioning_28 h ON dsp.order_date = h.order_date AND dsp.brand = h.brand
LEFT JOIN eta_7 i ON dsp.order_date = i.order_date AND dsp.brand = i.brand
LEFT JOIN eta_28 j ON dsp.order_date = j.order_date AND dsp.brand = j.brand
LEFT JOIN storefronts_7 k ON dsp.order_date = k.order_date AND dsp.brand = k.brand
LEFT JOIN storefronts_28 l ON dsp.order_date = l.order_date AND dsp.brand = l.brand
LEFT JOIN marketing_7 m ON dsp.order_date = m.order_date AND dsp.brand = m.brand
LEFT JOIN marketing_28 n ON dsp.order_date = n.order_date AND dsp.brand = n.brand
LEFT JOIN sales_7 o ON dsp.order_date = o.order_date AND dsp.brand = o.brand
LEFT JOIN sales_28 p ON dsp.order_date = p.order_date AND dsp.brand = p.brand
LEFT JOIN sales_per_store_7 q ON dsp.order_date = q.order_date AND dsp.brand = q.brand
LEFT JOIN sales_per_store_28 r ON dsp.order_date = r.order_date AND dsp.brand = r.brand
LEFT JOIN marketing_per_store_7 s ON dsp.order_date = s.order_date AND dsp.brand = s.brand
LEFT JOIN marketing_per_store_28 t ON dsp.order_date = t.order_date AND dsp.brand = t.brand
LEFT JOIN orders_per_store_7 u ON dsp.order_date = u.order_date AND dsp.brand = u.brand
LEFT JOIN orders_per_store_28 v ON dsp.order_date = v.order_date AND dsp.brand = v.brand
LEFT JOIN roi_7 w ON dsp.order_date = w.order_date AND dsp.brand = w.brand
LEFT JOIN roi_28 x ON dsp.order_date = x.order_date AND dsp.brand = x.brand

;



--select * from doris_temp where order_date = '2021-02-10' order by brand;

--Select count(*) from doris_temp where order_date = '2021-02-10'; --1221
--select count(distinct order_date + brand) from brand_optimisation.daily_storefront_performance where order_date = '2021-02-10';  --59

/*
select distinct brand_type from brand_optimisation.daily_storefront_performance;


select brand, kitchen_code, min(order_date) from kitchen.order
where lower(brand) like '%man%fries%'
and kitchen_code = 'SEA14-1'
GROUP BY 1,2
;

*/
