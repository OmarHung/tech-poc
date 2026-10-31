//! The shared Lua scripts (`../shared/lua/`), compiled into the binary. Each is sent with
//! common.lua in front of it, exactly like every other language version of this PoC.

use std::sync::LazyLock;

macro_rules! lua {
    ($name:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../shared/lua/",
            $name,
            ".lua"
        ))
    };
}

const COMMON: &str = lua!("common");

/// One state transition. Runs atomically in Redis, so it is safe from any number of instances.
pub struct SaleScript {
    pub name: &'static str,
    source: String,
    script: redis::Script,
}

impl SaleScript {
    fn new(name: &'static str, body: &str) -> Self {
        let source = format!("{COMMON}\n{body}");
        let script = redis::Script::new(&source);
        Self {
            name,
            source,
            script,
        }
    }

    /// common.lua, a newline, then the script itself.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Sent as EVALSHA; the redis crate falls back to loading the script when Redis does not have it.
    pub(crate) fn script(&self) -> &redis::Script {
        &self.script
    }
}

pub struct SaleScripts {
    pub init: SaleScript,
    pub join: SaleScript,
    pub ticket: SaleScript,
    pub admit: SaleScript,
    pub checkout: SaleScript,
    pub order: SaleScript,
    pub pay: SaleScript,
    pub expire_orders: SaleScript,
    pub audit: SaleScript,
    pub tickets: SaleScript,
    pub watch: SaleScript,
}

pub static SCRIPTS: LazyLock<SaleScripts> = LazyLock::new(|| SaleScripts {
    init: SaleScript::new("init", lua!("init")),
    join: SaleScript::new("join", lua!("join")),
    ticket: SaleScript::new("ticket", lua!("ticket")),
    admit: SaleScript::new("admit", lua!("admit")),
    checkout: SaleScript::new("checkout", lua!("checkout")),
    order: SaleScript::new("order", lua!("order")),
    pay: SaleScript::new("pay", lua!("pay")),
    expire_orders: SaleScript::new("expire_orders", lua!("expire_orders")),
    audit: SaleScript::new("audit", lua!("audit")),
    tickets: SaleScript::new("tickets", lua!("tickets")),
    watch: SaleScript::new("watch", lua!("watch")),
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_script_starts_with_the_shared_helpers() {
        let all = [
            &SCRIPTS.init,
            &SCRIPTS.join,
            &SCRIPTS.ticket,
            &SCRIPTS.admit,
            &SCRIPTS.checkout,
            &SCRIPTS.order,
            &SCRIPTS.pay,
            &SCRIPTS.expire_orders,
            &SCRIPTS.audit,
            &SCRIPTS.tickets,
            &SCRIPTS.watch,
        ];
        for script in all {
            let source = script.source();
            assert!(source.starts_with(COMMON), "{}", script.name);
            assert!(source.len() > COMMON.len() + 1, "{}", script.name);
            assert_eq!(
                &source[COMMON.len()..=COMMON.len()],
                "\n",
                "{}",
                script.name
            );
        }
        assert!(
            SCRIPTS
                .join
                .source()
                .contains("ARGV: prefix, userId, newTicketId, now")
        );
        assert_ne!(
            SCRIPTS.join.script().get_hash(),
            SCRIPTS.ticket.script().get_hash()
        );
    }
}
