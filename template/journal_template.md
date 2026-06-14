---
number headings: off
tags:
  - Journal
date: {{date}}
---
1. ==인정받으려 하지 말라==. 내 초라함이 드러날 것이다.
2. ==비껴말하라==. 단도직입은 오히려 전달되지 않는다. 
3. ==일을 몰고가라==. 아니면 일이 나를 몰고갈 것이다.
# Journal
Orient - Step - Learn
https://share.google/aimode/6gPjnCDVm9L86mvqY
- [ ] 
📌🔥🩸💕
# 일정
```tasks
path does not include {{query.file.path}}
tags regex matches /#일정|#교육/
happens {{query.file.property('date')}}
```
# In Progress
````````ad-success
title: Stay Focused
icon: pen-nib

```tasks
(status.type is IN_PROGRESS) OR (done after {{query.file.property('date')}})
# exclude sub-items
NOT (created after {{query.file.property('date')}})
show tree
```
````````
# Done
````````ad-done
title: Nailed It
icon: skull-crossbones
color: 100, 100, 100
collapse: true

```tasks
done {{query.file.property('date')}}
```
````````
# DoIt
````````ad-info
title: Jump Start
icon: paper-plane 

```tasks
path does not include {{query.file.path}}
(status.type is TODO) OR (done afte {{query.file.property('date')}})
NOT (done in {{query.file.property('date')}})
(created on {{query.file.property('date')}}) OR (created before {{query.file.property('date')}}) OR (no created date)
tags do not include #일정 
#filter by function task.urgency > 10
#group by status
sort by urgency
limit 4
show tree
```
````````
# 꾸준히
````````ad-hint
title: 사람들은 1년에 할 수 있는 일은 과대평가하고, 10년에 할 수 있는 일은 과소평가한다
icon: paper-plane 
```tasks
tags includes #꾸준히
not done
show tree
```
````````
# Residual ToDo
``````ad-seealso
title: Life is what happens when you’re busy making other plans.
icon: pagelines
color: 100, 100, 100
collapse: true

```tasks
(status.type is TODO) OR (done after {{query.file.property('date')}})
(created on {{query.file.property('date')}}) OR (created before {{query.file.property('date')}}) OR (no created date)
priority is above lowest
description regex does not match /^$/
group by function task.happens.format('%%0%% YYYY-MM-DD dd', '%%999' + task.priorityNumber + '%% No date & ' + task.priorityName); 
sort by urgency reverse
sort by created
show tree
```
``````
# Recently modified pages
``````ad-hint
title: 최근 변경 페이지
```dataview
TABLE file.mtime as Modified
SORT file.mtime desc
LIMIT 20
```
``````
# 요즘
````````ad-success
title: Stay Focused
icon: pen-nib

```tasks
tags includes #요즘
not done
show tree
```
````````
