SELECT SUM (n.n_regionkey) 
FROM nation AS n, region AS r 
WHERE (n.n_regionkey = r.r_regionkey) AND (n.n_name = 'UNITED STATES') 
GROUP BY n.n_regionkey
