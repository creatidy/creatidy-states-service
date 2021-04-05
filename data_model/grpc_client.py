import grpc

# import the generated classes
import protobufs.transactions_pb2
import protobufs.transactions_pb2_grpc
import protobufs.currency_rates_pb2
import protobufs.currency_rates_pb2_grpc

from datetime import datetime


class Transactions:

    def __init__(self):
        # open a gRPC channel
        self.channel = grpc.insecure_channel('localhost:50051')

        # create a stub (client)
        self.stub = protobufs.transactions_pb2_grpc.TransactionsStub(self.channel)

    def get_transactions(self):
        # create a valid request message
        request_message = protobufs.transactions_pb2.SearchRequest(query='test', page_number=2, result_per_page=3)

        # make the call
        response = self.stub.GetList(request_message)
        return response.transaction


class Rates:
    def __init__(self):
        # open a gRPC channel
        self.channel = grpc.insecure_channel('localhost:50052')

        # create a stub (client)
        self.stub = protobufs.currency_rates_pb2_grpc.RatesStub(self.channel)

    def get_rates_pln(self, sell, transaction_date: datetime):
        # create a valid request message

        transaction_timestamp = protobufs.currency_rates_pb2.google_dot_protobuf_dot_timestamp__pb2.Timestamp()
        transaction_timestamp.seconds = int(transaction_date.timestamp())
        request_message = protobufs.currency_rates_pb2.RateRequest(sell=sell, buy='PLN',
                                                                   timestamp=transaction_timestamp)

        # make the call
        response = self.stub.GetRate(request_message)
        return response
