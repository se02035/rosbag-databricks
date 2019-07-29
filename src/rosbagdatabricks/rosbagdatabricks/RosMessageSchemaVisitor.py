from rosbagdatabricks.RosMessageParserVisitor import RosMessageParserVisitor
from rosbagdatabricks.RosMessageParser import RosMessageParser


class RosMessageSchemaVisitor(RosMessageParserVisitor):
    fields = {}

    def visitField_declaration(self, ctx):
        if not self._ancestorIsHeader(ctx):
            field_name = ctx.getChild(1).getText()
            field_type = ctx.getChild(0).getText()
            self.fields[field_name] = field_type

    def _ancestorIsHeader(self, node):
        while(node.parentCtx):
            if isinstance(node, RosMessageParser.Rosbag_nested_messageContext) and node.getChild(1).getChild(2).getText() != 'Header':
                return False
            node = node.parentCtx
        return True