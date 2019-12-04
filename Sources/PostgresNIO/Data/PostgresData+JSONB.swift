import Foundation


extension PostgresData {
    public var jsonb: Data? {
        guard var value = self.value else {
            return nil
        }

        guard let versionBytes = value.readBytes(length: jsonBVersionBytes.count), [UInt8](versionBytes) == jsonBVersionBytes else {
            return nil
        }

        guard let dataBytes = value.readBytes(length: value.readableBytes) else {
            return nil
        }

        return Data(dataBytes)
    }

    public func jsonb<T>(as type: T.Type) throws -> T? where T: Decodable {
        guard let jsonData = jsonb else {
            return nil
        }

        return try JSONDecoder().decode(T.self, from: jsonData)
    }
}

public protocol PostgresJSONBCodable: Codable, PostgresBind, PostgresDataConvertible { }

extension PostgresJSONBCodable {
    public static var postgresDataType: PostgresDataType {
        return .jsonb
    }
    
    public init?(postgresData: PostgresData) {
        guard let value = try? postgresData.jsonb(as: Self.self) else {
            return nil
        }
        self = value
    }
    
    public func postgresData(type: PostgresDataType) -> ByteBuffer? {
        switch type {
        case .jsonb:
            guard let data = try? JSONEncoder().encode(self) else {
                return nil
            }
            var buffer = ByteBufferAllocator().buffer(capacity: jsonBVersionBytes.count + data.count)
            buffer.writeBytes(jsonBVersionBytes)
            buffer.writeBytes(data)
            return buffer
        default:
            return nil
        }
    }
}

private let jsonBVersionBytes: [UInt8] = [0x01]
