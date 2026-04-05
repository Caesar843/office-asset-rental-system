create table asset_system.assets
(
    id          int          not null
        primary key,
    asset_name  varchar(255) null,
    category_id int          null,
    qr_code     varchar(255) null,
    status      tinyint      null,
    location    varchar(255) null,
    constraint fk_assets_category
        foreign key (category_id) references asset_system.categories (id)
);

create table asset_system.categories
(
    id          int          not null
        primary key,
    cat_name    varchar(255) null,
    description text         null
);

create table asset_system.operation_records
(
    op_id     int          not null
        primary key,
    asset_id  int          null,
    user_id   int          null,
    op_type   varchar(255) null,
    op_time   datetime     null,
    hw_seq    varchar(255) null,
    hw_result varchar(255) null,
    due_time  datetime     null,
    constraint fk_operation_asset
        foreign key (asset_id) references asset_system.assets (id),
    constraint fk_operation_user
        foreign key (user_id) references asset_system.users (user_id)
);

create table asset_system.users
(
    user_id      int             not null
        primary key,
    user_name    varchar(255)    null,
    student_id   varchar(255)    null,
    credit_score int default 100 null,
    status       tinyint         null
);

