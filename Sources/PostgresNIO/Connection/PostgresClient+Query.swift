import NIO
import Logging

extension PostgresDatabase {
    public func query(_ string: String, _ binds: [PostgresData] = []) -> EventLoopFuture<[PostgresRow]> {
        var rows: [PostgresRow] = []
        return query(string, binds) { rows.append($0) }.map { rows }
    }
    
    public func query(_ string: String, _ binds: [PostgresData] = [], _ onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        let query = PostgresParameterizedQuery(query: string, binds: binds, onRow: onRow)
        return self.send(query, logger: self.logger)
    }
}

// MARK: Private

private final class PostgresParameterizedQuery: PostgresRequest {
    let query: String
    let binds: [PostgresData]
    var onRow: (PostgresRow) throws -> ()
    var rowLookupTable: PostgresRow.LookupTable?
    var resultFormatCodes: [PostgresFormatCode]
    var logger: Logger?
    
    init(
        query: String,
        binds: [PostgresData],
        onRow: @escaping (PostgresRow) throws -> ()
    ) {
        self.query = query
        self.binds = binds
        self.onRow = onRow
        self.resultFormatCodes = [.binary]
    }
    
    func log(to logger: Logger) {
        self.logger = logger
        logger.debug("\(self.query) \(self.binds)")
    }
    
    func respond(to message: PostgresMessage) throws -> [PostgresMessage]? {
        if case .error = message.identifier {
            // we should continue after errors
            return []
        }
        switch message.identifier {
        case .bindComplete:
            return []
        case .dataRow:
            let data = try PostgresMessage.DataRow(message: message)
            guard let rowLookupTable = self.rowLookupTable else { fatalError() }
            let row = PostgresRow(dataRow: data, lookupTable: rowLookupTable)
            try onRow(row)
            return []
        case .rowDescription:
            let row = try PostgresMessage.RowDescription(message: message)
            self.rowLookupTable = PostgresRow.LookupTable(
                rowDescription: row,
                resultFormat: self.resultFormatCodes
            )
            return []
        case .noData:
            return []
        case .parseComplete:
            return []
        case .parameterDescription:
            let params = try PostgresMessage.ParameterDescription(message: message)
            if params.dataTypes.count != self.binds.count {
                self.logger!.warning("Expected parameters count (\(params.dataTypes.count)) does not equal binds count (\(binds.count))")
            } else {
                for (i, item) in zip(params.dataTypes, self.binds).enumerated() {
                    if item.0 != item.1.type {
                        self.logger!.warning("bind $\(i + 1) type (\(item.1.type)) does not match expected parameter type (\(item.0))")
                    }
                }
            }
            return []
        case .commandComplete:
            return []
        case .notice:
            return []
        case .notificationResponse:
            return []
        case .readyForQuery:
            return nil
        default: throw PostgresError.protocol("Unexpected message during query: \(message)")
        }
    }
    
    func start() throws -> [PostgresMessage] {
        let parse = PostgresMessage.Parse(
            statementName: "",
            query: self.query,
            parameterTypes: self.binds.map { $0.type }
        )
        let describe = PostgresMessage.Describe(
            command: .statement,
            name: ""
        )
        let bind = PostgresMessage.Bind(
            portalName: "",
            statementName: "",
            parameterFormatCodes: self.binds.map { $0.formatCode },
            parameters: self.binds.map { .init(value: $0.value) },
            resultFormatCodes: self.resultFormatCodes
        )
        let execute = PostgresMessage.Execute(
            portalName: "",
            maxRows: 0
        )
        
        let sync = PostgresMessage.Sync()
        return try [parse.message(), describe.message(), bind.message(), execute.message(), sync.message()]
    }
}
