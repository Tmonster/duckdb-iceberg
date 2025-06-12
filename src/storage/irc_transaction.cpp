#include "storage/irc_transaction.hpp"
#include "storage/irc_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

namespace duckdb {

IRCTransaction::IRCTransaction(IRCatalog &ic_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), access_mode(ic_catalog.access_mode) {
	//	connection = ICConnection::Open(ic_catalog.path);
}

IRCTransaction::~IRCTransaction() = default;

void IRCTransaction::Start() {
	transaction_state = IRCTransactionState::TRANSACTION_NOT_YET_STARTED;
}
void IRCTransaction::Commit() {
	if (transaction_state == IRCTransactionState::TRANSACTION_STARTED) {
		transaction_state = IRCTransactionState::TRANSACTION_FINISHED;
		//		connection.Execute("COMMIT");
	}
}
void IRCTransaction::Rollback() {
	if (transaction_state == IRCTransactionState::TRANSACTION_STARTED) {
		transaction_state = IRCTransactionState::TRANSACTION_FINISHED;
		//		connection.Execute("ROLLBACK");
	}
}

IRCTransaction &IRCTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<IRCTransaction>();
}

} // namespace duckdb
