# Pig 简介

---

# Agenda

- Hadoop Overview
- Pig: Basic Intro
- A Demo Product
- Others

---

# MapReduce Workflow

- Challenge: how many page view per user, given user session logs?

- Input: key=row, value=user session log

- Map: output key=JCN_SESS_ID, value=1

- Shuffle: sort by JCN_SESS_ID

- Reduce: for each JCN_SESS_ID, sum

- Output: JCN_SESS_ID, page view count

---

![MapReduceFlow](./mrwf.png)

---

# What is Pig?

- A Translator. Translate high-level language program to low-level map-reduce programs;
- A Platform.   Infrastructure for evalating programs;
- A Language.   Pig Latin;

---

# Pig Latin Tutorial

A Simple Example

    !mysql
    A = load 'passwd' using PigStorage(':');  -- load the passwd file 
    B = foreach A generate $0 as id;  -- extract the user IDs 
    store B into ‘id.out’;  -- write the results to a file name id.out

How to find the top 100 most visited sites by users aged 18 to 25??

    !mysql
    Users = LOAD ‘users’ AS (name, age);
    Fltrd = FILTER Users by age >= 18 and age <= 25; 
    Pages = LOAD ‘pages’ AS (user, url);
    Jnd = JOIN Fltrd BY name, Pages BY user;
    Grpd = GROUP Jnd by url;
    Smmd = FOREACH Grpd GENERATE group, COUNT(Jnd) AS clicks;
    Srtd = ORDER Smmd BY clicks;
    Top100 = LIMIT Srtd 100;
    STORE Top100 INTO ‘top100sites’; 

---

# Native Hadoop M/R Program

![MRPROGRAM](./complex_mrprogram.png)

---

# Pig Latin Data Types

- int, long, float, double, chararray(string), bytearray
- tuple (alice, bob)
- bag   {(alice, bob), (alice, tom)}
- map   ['likes'#'lakers', 'age'#22]

---

# Expressions

a -> (f1:int, f2:bag{t:tuple(n1:int, n2:int)}, f3:map[])

(1, {(2,3), (4,6)}, ['yahoo'#'mail'])

f1 or $0 = 1

f2 or $1 = {(2,3), (4,6)}

f2.$0 = {(2), (4)}

f3 or $2 = ['yahoo'#'mail']

f3#'yahoo' = 'mail'

SUM(f2.$1) = 9 (3+6)

COUNT($2) = 2L

- ==, !=, >, >=, <, <=
- matches
- AND OR NOT
- (Condition ? exp1: exp2)

---

# Operators

- foreach
- distinct, filter, group
- join (inner, outer)
- order by
- etc.

---

# Built In Functions

- avg, count, max, min, size, sum
- isempty
- concat, tokenize
- abs, cos, sin, etc.
- indexof, substring, trim, etc.

---

# Examples

- Region User Stat

    Given Jcn Session Logs,
    
    <http://bi.jcndev.com/cubey/sql_query/guang_region_user_stat.html>

---

# Data

    !python
    {"uid":"-","endtime":"2013-01-29 22:05:36","ref":"http://tf.360.cn/e/wb?_=76b6fa2e03fe712e&mid=","uids":"-","starttime":"2013-01-29 22:05:36","paths":[{"status_code":"200","cookie":"
    bgt=20130112150311; JCN_SESS_ID=e720d7f6-b6d5-4618-b082-f9d6148705f9; JID=02Qc9FDxCq+YPRScBnysAg==; __utma=34408359.1336847777.1357974199.1357974199.1357974199.1; __utmz=34408359.1357974199.
        1.1.utmcsr=tf.360.cn|utmccn=(referral)|utmcmd=referral|utmcct=/e/wb","ref":"http://tf.360.cn/e/wb?_=76b6fa2e03fe712e&mid=","time":"2013-01-29 22:05:36","url":"/pickxks?from=ad_2pick1_dj004"}
    ],"browser":"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1","ssid":"e720d7f6-b6d5-4618-b082-f9d6148705f9","size":"1","ip":"121.12.16.249
        "}

---

# Pig Loader

JsonLoader

Self-Contain JsonLoader need specify schemes!

We Use elephant-bird which comes from twitter.


    !mysql
    register 'elephant-bird-core.jar';
    register 'elephant-bird-pig.jar';
    register 'json-simple-1.1.jar';

    a = load '$INPUT' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

---

# Internal Data Structure

Bag : List of Tuple, {(a, b, c), (1, 2, 3)}

Our : {([status_code#"200",time#"2013-01-29 22:05:36",url#"/pickxks?from=ad_2pick1_dj004"])}

tuple has only one item : a map, inconvenient for process

UDF : User Define Function

    !java
    public class SessMapBagToTupleBag extends EvalFunc<DataBag> {
    String[] keys;
    public SessMapBagToTupleBag(String keylist) {
        keys = keylist.split(",");
    }
    @Override
    public DataBag exec(Tuple input) throws IOException {
        try {
            DataBag bag = BagFactory.getInstance().newDefaultBag();
            DataBag oldbag = (DataBag)input.get(0);
            Iterator it = oldbag.iterator();

            while (it.hasNext()) {
                List<Object> items = new ArrayList<Object>();
                Tuple t = (Tuple)it.next();
                Map<String, String> m = (Map)t.get(0);
                for (String key : keys) {
                    items.add(m.get(key));
                }
                Tuple t2 = TupleFactory.getInstance().newTuple(items);
                bag.add(t2);
            }
            return bag;
        } catch (Exception ee) {
            throw new RuntimeException("Error while SessMapBagToTupleBag", ee);
        }
    }
    }

---

# Internal Data Structure (Cont.)

UDF: Pig Supports Java, Python, Ruby, etc.

    !mysql
    register 'piggybank.jar';
    define GO org.apache.pig.piggybank.evaluation.jcn.SessMapBagToTupleBag('status_code,cookie,time,ref,url');

    b = foreach a generate (chararray) $0#'uid' as uid, $0#'endtime' as endtime,
      $0#'starttime' as starttime, $0#'uids' as uids, $0#'ref' as ref,
        $0#'browser' as browser, $0#'ssid' as ssid, $0#'size' as size, $0#'ip' as ip,
          GO($0#'paths') as paths;

---

# Filter

Python UDF : session filter function

    !mysql
    register 'jcn_udf.py' using jython as myudf;
    c = filter b by myudf.sessFiltered(uids, ip) == 0 and starttime >= '$DAY2';

Python Code

    !python
    @outputSchema("is_filterd:int")
    def sessFiltered(uids, ip):
    for uid in uids.split(','):
        if SessionFilter.uid_filter(uid):
            return 1
    if SessionFilter.ip_filter(ip):
        return 1
    return 0

---

# Generate Data for every session

    !mysql
    d = foreach c generate SUBSTRING(starttime, 0, 10) as day,
             myudf.province(ip) as province, 1 as uv, (int)size as pv,
             myudf.newuser(paths.$1, '$DAY') as newuser,
             myudf.urlsearch(paths.$4, '/favourite/add') as fav_cnt,
             myudf.urlsearch(paths.$4, '/skip.htm') as oc_cnt,
             myudf.device(browser) as device;

Python UDF Code:

    !python
    @outputSchema("is_new:int")
    def newuser(cookies, day):
        cookies = list(cookies)
        if not cookies:
            return 0
        cookie = cookies[0][0]
        m = parseCookie2Dict(cookie)
        bgt = m.get('bgt', '')
        if bgt and bgt >= day:
            return 1
        return 0

---

# Generate Data for db table

    !mysql
    e = foreach d generate day, province, uv, pv,
      (newuser == 1 ? 1 : 0) as is_new_user,
      (newuser == 1 ? 0 : 1) as is_old_user,
      (fav_cnt > 0 ? 1 : 0) as has_fav, fav_cnt,
      (oc_cnt > 0 ? 1 : 0) as has_oc, oc_cnt,
      (device == '0' ? 1 : 0) as pc,
      (device == '0' ? 0 : 1) as mobile;
    

---

# Group by province

    !mysql
    f = group e by (day, province);
    g = foreach f generate CONCAT(group.day, ' 00:00:00'), group.province, SUM(e.uv), SUM(e.pv),
      SUM(e.is_new_user), SUM(e.is_old_user), SUM(e.has_fav), SUM(e.fav_cnt),
      SUM(e.has_oc), SUM(e.oc_cnt), SUM(e.pc), SUM(e.mobile);
    store g into '$HADOOP_OUTPUT';

---

# pig run

    !bash
    pig -Dmapred.cache.files=hdfs://sdl-job1/user/maoxing.xu/pig/special_uids#uid_fromdb.txt,hdfs://sdl-job1/user/maoxing.xu/pig/ipblk_rgn_code.txt#ipblk_rgn_code.txt,hdfs://sdl-job1/user/maoxing.xu/pig/geo_code_labels.txt#geo_code_labels.txt \
    -Dmapred.create.symlink=yes \
    -param INPUT=/logs/guang/nginxlog_sess/$day_hdp/allday \
    -param DAY=$day_py \
    -param DAY2=$day_hdp \
    -param HADOOP_OUTPUT=$hdp_path \
    ./jcnsess_load.pig

---

# export data from hadoop to mysql

    !bash
    sqoop-export --connect jdbc:mysql://192.168.10.71/guangbi --username guangbi --password guangbi --table region_user_stat --export-dir $hdp_path --input-fields-terminated-by '\t'

---

# How many Code?

shell script + pig script + python udf script = 125 lines

---

# Resources

- <http://pig.apache.org/docs/r0.10.0/basic.html>
- <http://data.linkedin.com/opensource/datafu> udf
- <https://github.com/kevinweil/elephant-bird> loader, protocolbuf/thrift

---

# Q/A
