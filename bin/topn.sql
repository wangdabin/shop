SELECT CASE
           WHEN mid_action.platform_type IS NOT NULL THEN mid_action.platform_type
           ELSE mid1_action.platform_type
       END AS platform_type,
       CASE
           WHEN mid_action.product_type IS NOT NULL THEN mid_action.product_type
           ELSE mid1_action.product_type
       END AS platform_type,
       CASE
           WHEN mid_action.view_number IS NOT NULL THEN mid_action.view_number
           ELSE 0
       END AS view_number,
       CASE
           WHEN mid1_action.sale_number IS NOT NULL THEN mid1_action.sale_number
           ELSE 0
       END AS sale_number
FROM
  (SELECT platform.type platform_type,
          product.type product_type,
          count(*) view_number
   FROM
     (SELECT *
      FROM action
      WHERE action.type = 1 AND action.createtime >= starttime AND action.createtime <= endtime) action
   LEFT OUTER JOIN platform ON action.platformid = platform.id
   LEFT OUTER JOIN goods ON action.itemid = goods.id
   LEFT OUTER JOIN product ON goods.productid = product.id
   GROUP BY platform.type,
            product.type) mid_action
FULL OUTER JOIN
  (SELECT platform.type platform_type,
          product.type product_type,
          count(*) sale_number
   FROM
     (SELECT *
      FROM action
      WHERE action.type = 0  AND action.createtime >= starttime AND action.createtime <= endtime) action
   LEFT OUTER JOIN platform ON action.platformid = platform.id
   LEFT OUTER JOIN purchase ON action.itemid = purchase.orderid
   LEFT OUTER JOIN goods ON purchase.goodsid = goods.id
   LEFT OUTER JOIN product ON goods.productid = product.id
   GROUP BY platform.type,product.type) mid1_action
ON (mid_action.platform_type = mid1_action.platform_type AND mid_action.product_type = mid1_action.product_type)