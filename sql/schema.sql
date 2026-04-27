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
    op_id             int          not null
        primary key,
    asset_id          int          null,
    user_id           int          null,
    op_type           varchar(255) null,
    op_time           datetime     null,
    hw_seq            varchar(255) null,
    hw_result         varchar(255) null,
    due_time          datetime     null,
    user_name         varchar(255) null,
    request_seq       bigint       null,
    request_id        varchar(64)  null,
    hw_sn             varchar(128) null,
    borrow_request_id varchar(64)  null,
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

create table asset_system.borrow_requests
(
    id                  int auto_increment
        primary key,
    request_id          varchar(64)  not null,
    asset_id            varchar(64)  not null,
    applicant_user_id   varchar(64)  not null,
    applicant_user_name varchar(128) not null,
    reason              text         null,
    requested_days      int          not null default 30,
    status              varchar(32)  not null,
    reviewer_user_id    varchar(64)  null,
    reviewer_user_name  varchar(128) null,
    review_comment      text         null,
    requested_at        varchar(32)  not null,
    reviewed_at         varchar(32)  null,
    consumed_at         varchar(32)  null,
    constraint uq_borrow_requests_request_id
        unique (request_id)
);

create index idx_borrow_requests_status
    on asset_system.borrow_requests (status);

create index idx_borrow_requests_asset_id
    on asset_system.borrow_requests (asset_id);

create index idx_borrow_requests_applicant_user_id
    on asset_system.borrow_requests (applicant_user_id);

create table asset_system.return_acceptance_records
(
    id                        int auto_increment
        primary key,
    asset_id                  varchar(64)  not null,
    acceptance_result         varchar(32)  not null,
    note                      text         null,
    accepted_by_user_id       varchar(64)  not null,
    accepted_by_user_name     varchar(128) not null,
    accepted_at               varchar(32)  not null,
    related_return_request_seq bigint      null,
    related_return_request_id varchar(64)  null,
    related_return_hw_seq     bigint       not null,
    constraint uq_return_acceptance_asset_hw_seq
        unique (asset_id, related_return_hw_seq)
);

create index idx_return_acceptance_result
    on asset_system.return_acceptance_records (acceptance_result);

create index idx_return_acceptance_user
    on asset_system.return_acceptance_records (accepted_by_user_id);

create index idx_return_acceptance_time
    on asset_system.return_acceptance_records (accepted_at);

