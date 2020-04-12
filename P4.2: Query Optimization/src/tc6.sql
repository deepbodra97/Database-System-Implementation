SELECT SUM (ps.ps_supplycost), s.s_suppkey 
FROM part AS p, supplier AS s, partsupp AS ps 
WHERE (p.p_partkey = ps.ps_partkey) AND (s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500.0) 
GROUP BY s.s_suppkey
