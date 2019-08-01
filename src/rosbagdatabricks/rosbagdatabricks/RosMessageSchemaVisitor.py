from rosbagdatabricks.RosMessageParserVisitor import RosMessageParserVisitor
from rosbagdatabricks.RosMessageParser import RosMessageParser
from pyspark.sql.types import *
import re

class RosMessageSchemaVisitor(RosMessageParserVisitor):

    def visitRosbag_input(self, ctx):
        ros_message = ctx.getChild(0)       
        schema = self.visitFieldDeclarationChildren(ros_message)

        return schema

    def visitFieldDeclarationChildren(self, node):
        result = {}
        n = node.getChildCount()
        for i in range(n):
            if not self.shouldVisitNextChild(node, result):
                return result
    
            c = node.getChild(i)
            if isinstance(c, RosMessageParser.Field_declarationContext):
                childResult = c.accept(self)
                result = self.aggregateDictionaryResult(result, childResult)
    
        return result

    def visitField_declaration(self, node):
        return { node.getChild(0).getText().split('/')[-1]: node.getChild(1).getText()}

    def aggregateDictionaryResult(self, aggregate, nextResult):
        aggregate.update(nextResult)
        return aggregate