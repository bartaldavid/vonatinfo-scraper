select *
from train_position
order by delay desc
limit 10;


select datetime(train_position.created_at, 'auto')
from train_position;


select count(*)
from train_position
where train_number = '11864';


select distinct train_position.relation
from train_position
where relation like '%Balatonfüred%';


select distinct train_position.elvira_id, min(datetime(created_at, 'auto'))
from train_position
where relation = 'Budapest-Déli - Keszthely'
group by train_position.elvira_id
order by 2;


create index train_number_idx on train_position (train_number);
create index train_relation_idx on train_position (relation);

select datetime(created_at, 'auto'), *
from train_position
where train_number = '55864'
order by created_at
;

select max(train_position.delay)
from train_position
where train_number = '55864';

select count(*), train_number, elvira_id
from train_position
where train_number in ('5511864', '55864', '5519714', '5511184', '5519704', '5511904')
  and date(created_at, 'auto') = '2025-06-09'
group by train_number, elvira_id;

-- 444 alapján  (19704, 11486, 11904 - Budapest-Déli - Székesfehérvár; Székesfehérvár - Balatonfüred; Balatonfüred - Tapolca),
-- vagyis a legvégén még csavartak rajta egyet, és Füred és Badacsontomaj között 11994-es szám alatt gurult,
-- hogy onnan pótlóbusz menjen tovább
select count(*), train_number, elvira_id, max(datetime(created_at, 'auto')), relation
from train_position
where train_number in ('5519704', '5511486', '5511904', '5511994')
group by train_number, elvira_id, relation;

--9714-es Vízipók, 3 vonatszámban ment le Balatonfüredre, ez 11184 lett Fehérvárig,
-- aztán ott átvariálták még egyszer, és 11974 lett belőle.
select count(*), train_number, elvira_id, max(datetime(created_at, 'auto')), relation
from train_position
where train_number in ('559714', '5511184', '5511974')
group by train_number, elvira_id, relation;

-- melyik vonat folytatja balatonfuredtol?
select distinct relation, train_position.train_number, min(datetime(created_at, 'auto')), min(created_at)
from train_position
where relation like '% - Balatonfüred'
  and created_at > 1749473631
group by relation, train_position.train_number
order by 4;

select * from train_position where train_number like '%11864%';


-- underscore-ral kezdodo elviraid-k
select max(delay),
       train_position.elvira_id,
       relation,
       train_number,
       min(datetime(created_at, 'auto')),
       min(created_at)

from train_position
where elvira_id
          like '\_%' escape '\'
group by train_position.elvira_id, relation, train_number
order by 5;


-- trains with no delay
select max(delay), train_position.elvira_id
from train_position
where menetvonal = 'MAV'
group by elvira_id
having max(delay) = 0;


-- weird train 2: _250607, _Déli-Székesfehérvár, 551484, min created at: 1749341331
select relation, train_position.train_number, elvira_id, max(created_at), max(delay)
from train_position
where created_at < 1749341331
  and relation like 'Budapest-Déli -%'
  and elvira_id like '%_250607'
group by relation, train_position.train_number, elvira_id
order by 4 desc