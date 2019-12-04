import Foundation

extension PostgresData {
    public var uuid: UUID? {
        guard var value = self.value else {
            return nil
        }
        
        return value.readUUID()
    }
}

extension UUID: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        return .uuid
    }
    
    public init?(postgresData: PostgresData) {
        guard let uuid = postgresData.uuid else {
            return nil
        }
        self = uuid
    }
}

extension UUID: PostgresBind {
    public func postgresData(type: PostgresDataType) -> ByteBuffer? {
        switch type {
        case .uuid:
            var buffer = ByteBufferAllocator().buffer(capacity: 16)
            buffer.writeBytes([
                self.uuid.0, self.uuid.1, self.uuid.2, self.uuid.3,
                self.uuid.4, self.uuid.5, self.uuid.6, self.uuid.7,
                self.uuid.8, self.uuid.9, self.uuid.10, self.uuid.11,
                self.uuid.12, self.uuid.13, self.uuid.14, self.uuid.15,
            ])
            return buffer
        default:
            return nil
        }
    }
}
