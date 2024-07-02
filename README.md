## Database system project

> metadata를 활용하여 Table의 정보를 저장하고, Table의 레코드 위치를 받아 블록 단위로 삽입, 삭제, 검색 기능 구현


Metadata의 저장만 MySQL 연동하여 사용
```
 create table Relation_metadata
	(relation_name		varchar(20) not null,
	number_of_attributes	int,
	storage_organization	varchar(20),
	location			varchar(255),
	primary key(relation_name));

create table Attribute_metadata
	(relation_name		varchar(20) not null,
	attribute_name		varchar(20) not null,
	domain_type		varchar(20),
	position			int,
	length			int,
	primary key(relation_name, attribute_name),
	foreign key(relation_name) references Relation_metadata(relation_name) on delete cascade);
```

1. 블록 단위 I/O 구현
2. 고정 길이 레코드
3. 테이블 생성 기능
4. 레코드의 삽입, 삭제, 검색 기능
5. Hash Join 구현
6. FreeList 기능 구현(미완)

