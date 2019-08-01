from rosbagdatabricks.RosMessageParserVisitor import RosMessageParserVisitor
from rosbagdatabricks.RosMessageParser import RosMessageParser
from pyspark.sql.types import *
import re

class RosMessageNestedSchemaVisitor(RosMessageParserVisitor):

    def visitRosbag_input(self, ctx):
        nested_schema = self.visitNestedMessageChildren(ctx)

        return nested_schema

    def visitNestedMessageChildren(self, node):
        result = {}
        n = node.getChildCount()
        for i in range(n):
            if not self.shouldVisitNextChild(node, result):
                return result
    
            c = node.getChild(i)

            if isinstance(c, RosMessageParser.Rosbag_nested_messageContext):
                nested_message_identifier = c.getChild(1).getText().split('/')[-1]
                childResult = self.visitChildren(c)

                result = self.aggregateDictionaryResult(result, childResult, nested_message_identifier)
    
        return result

    def aggregateDictionaryResult(self, aggregate, nextResult, nested_message_identifier):
        aggregate.update({nested_message_identifier: nextResult})
        return aggregate

    def defaultResult(self):
        return []

    def aggregateResult(self, aggregate, nextResult):
        if nextResult:
            aggregate.append(nextResult)
        return aggregate
    
    def visitChildren(self, node):
        result = self.defaultResult()
        n = node.getChildCount()
        for i in range(n):
            if not self.shouldVisitNextChild(node, result):
                return result

            c = node.getChild(i)
            childResult = c.accept(self)
            result = self.aggregateResult(result, childResult)

        return result

    def visitField_declaration(self, node):
        return { node.getChild(0).getText().split('/')[-1]: node.getChild(1).getText()}