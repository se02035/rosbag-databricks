from rosbagdatabricks.RosMessageParserVisitor import RosMessageParserVisitor
from rosbagdatabricks.RosMessageParser import RosMessageParser
from pyspark.sql.types import StructType, StringType

import uuid

class RosMessageSchemaVisitor(RosMessageParserVisitor):

    def visitRosbag_input(self, ctx):
        ros_message = ctx.getChild(0)
        struct_names = self.visitFieldDeclarationChildren(ros_message)
        
        struct = self.visitNestedMessageChildren(ctx)
        return struct

    def visitFieldDeclarationChildren(self, node):
        result = {}
        n = node.getChildCount()
        for i in range(n):
            if not self.shouldVisitNextChild(node, result):
                return result
    
            c = node.getChild(i)
            if isinstance(c, RosMessageParser.Field_declarationContext):
                childResult = c.accept(self)
                result = self.aggregateStructNames(result, childResult)
    
        return result

    def visitField_declaration(self, node):
        return { 'name': node.getChild(1).getText(), 'type': node.getChild(0).getText() }

    def aggregateStructNames(self, aggregate, nextResult):
        aggregate.update(nextResult)
        return aggregate

    def visitNestedMessageChildren(self, node):
        result = StructType()
        n = node.getChildCount()
        for i in range(n):
            if not self.shouldVisitNextChild(node, result):
                return result
    
            c = node.getChild(i)
            if isinstance(c, RosMessageParser.Rosbag_nested_messageContext):
                childResult = c.accept(self)
                nested_message_identifier = c.getChild(1).getChild(2).getText()
                result = self.aggregateStruct(result, childResult, nested_message_identifier)
    
        return result

    def aggregateStruct(self, aggregate, nextResult, nested_message_identifier):
        return aggregate.add(nested_message_identifier, nextResult, True)

    def aggregateFieldNames(self, aggregate, nextResult):
        aggregate.append(nextResult)
        return aggregate

    def visitRosbag_nested_message(self, ctx):
        return self.visitRosMessageChildren(ctx)
    
    def aggregateFieldNames(self, aggregate, nextResult):
        aggregate.append(nextResult)
        return aggregate

    def visitRosMessageChildren(self, node):
        result = self.defaultResult()
        n = node.getChildCount()
        for i in range(n):
            if not self.shouldVisitNextChild(node, result):
                return result
    
            c = node.getChild(i)
            if isinstance(c, RosMessageParser.Ros_messageContext):
                childResult = c.accept(self)
                result = self.aggregateResult(result, childResult)
    
        return result
    
    def visitRos_message(self, ctx):
        return self.visitFieldDeclarationStructChildren(ctx)

    def visitFieldDeclarationStructChildren(self, node):
        result = StructType()
        n = node.getChildCount()
        for i in range(n):
            if not self.shouldVisitNextChild(node, result):
                return result
    
            c = node.getChild(i)
            if isinstance(c, RosMessageParser.Field_declarationContext):
                childResult = c.accept(self)
                result = self.aggregateField(result, childResult)
    
        return result
    
    def aggregateField(self, aggregate, nextResult):
        aggregate.add(nextResult['name'], StringType(), True)
        return aggregate