-define(NAMESPACE, 'namespace/subspace').
-define(EPG_DB_REF, progressor_db).
-define(EPG_POOL, default_pool).
-define(REPL_SLOT_NAME, "cdc_slot_default").
-define(PRG_LIFECYCLE_TOPIC, <<"prg_lifecycle_topic">>).
-define(PRG_EVENTSINK_TOPIC, <<"prg_eventsink_topic">>).
-define(CDC_LIFECYCLE_TOPIC, <<"cdc_lifecycle_topic">>).
-define(CDC_EVENTSINK_TOPIC, <<"cdc_eventsink_topic">>).
-define(BROKERS, [{"kafka1", 9092}, {"kafka2", 9092}, {"kafka3", 9092}]).
-define(DEFAULT_KAFKA_CLIENT, default_kafka_client).
-define(BROKERS_INVALID, [{"kafka1", 29099}]).
-define(INVALID_KAFKA_CLIENT, invalid_kafka_client).
-define(INVALID_CDC_CONF, [
    {streams, #{
        ?EPG_DB_REF => #{
            ?REPL_SLOT_NAME => #{
                ?NAMESPACE => #{
                    kafka_client => ?INVALID_KAFKA_CLIENT,
                    eventsink_topic => ?CDC_EVENTSINK_TOPIC,
                    lifecycle_topic => ?CDC_LIFECYCLE_TOPIC
                }
            }
        }
    }},
    {resend_timeout, 100},
    {max_retries, 1},
    {reconnect_timeout, 1000}
]).
-define(DEFAULT_CDC_CONF, [
    {streams, #{
        ?EPG_DB_REF => #{
            ?REPL_SLOT_NAME => #{
                ?NAMESPACE => #{
                    kafka_client => ?DEFAULT_KAFKA_CLIENT,
                    eventsink_topic => ?CDC_EVENTSINK_TOPIC,
                    lifecycle_topic => ?CDC_LIFECYCLE_TOPIC
                }
            }
        }
    }}
]).
-define(DEFAULT_EPG_CONF, [
    {databases, #{
        ?EPG_DB_REF => #{
            host => "postgres",
            port => 5432,
            database => "progressor_db",
            username => "progressor",
            password => "progressor"
        }
    }},
    {pools, #{
        ?EPG_POOL => #{
            database => ?EPG_DB_REF,
            size => 10
        }
    }}
]).

-define(DEFAULT_PRG_CONF, [
    {namespaces, #{
        ?NAMESPACE => #{
            storage => #{
                client => prg_pg_backend,
                options => #{pool => ?EPG_POOL}
            },
            processor => #{
                client => cdc_prg_processor,
                options => #{}
            },
            notifier => #{
                client => ?DEFAULT_KAFKA_CLIENT,
                options => #{
                    topic => ?PRG_EVENTSINK_TOPIC,
                    lifecycle_topic => ?PRG_LIFECYCLE_TOPIC
                }
            }
        }
    }}
]).

-define(DEFAULT_BROD_CONF, [
    {clients, [
        {?DEFAULT_KAFKA_CLIENT, [
            {endpoints, ?BROKERS},
            {auto_start_producers, true},
            {default_producer_config, []}
        ]},
        {?INVALID_KAFKA_CLIENT, [
            {endpoints, ?BROKERS_INVALID},
            {auto_start_producers, true},
            {default_producer_config, []}
        ]}
    ]}
]).
