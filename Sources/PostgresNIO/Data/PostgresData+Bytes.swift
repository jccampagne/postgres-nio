import struct Foundation.Data
import NIOFoundationCompat

extension PostgresData {
    public init<Bytes>(bytes: Bytes)
        where Bytes: Sequence, Bytes.Element == UInt8
    {
        var buffer = ByteBufferAllocator().buffer(capacity: 1)
        buffer.writeBytes(bytes)
        self.init(type: .bytea, formatCode: .binary, value: buffer)
    }

    public var bytes: [UInt8]? {
        guard var value = self.value else {
            return nil
        }
        guard let bytes = value.readBytes(length: value.readableBytes) else {
            return nil
        }
        return bytes
    }
}

extension Data: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        return .bytea
    }

    public var postgresData: PostgresData? {
        return .init(bytes: self)
    }

    public init?(postgresData: PostgresData) {
        guard let bytes = postgresData.bytes else {
            return nil
        }
        self.init(bytes)
    }
}

extension Data: PostgresBind {
    public func postgresData(type: PostgresDataType) -> ByteBuffer? {
        switch type {
        case .bytea:
            var buffer = ByteBufferAllocator().buffer(capacity: self.count)
            buffer.writeBytes(self)
            return buffer
        default:
            return nil
        }
    }
}
