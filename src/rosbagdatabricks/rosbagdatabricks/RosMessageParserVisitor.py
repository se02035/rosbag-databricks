# Generated from RosMessageParser.g4 by ANTLR 4.7.2
from antlr4 import *

# This class defines a complete generic visitor for a parse tree produced by RosMessageParser.

class RosMessageParserVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by RosMessageParser#ros_file_input.
    def visitRos_file_input(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#ros_message_input.
    def visitRos_message_input(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#ros_action_input.
    def visitRos_action_input(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#ros_service_input.
    def visitRos_service_input(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#rosbag_input.
    def visitRosbag_input(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#rosbag_nested_message.
    def visitRosbag_nested_message(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#ros_message.
    def visitRos_message(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#field_declaration.
    def visitField_declaration(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#constant_declaration.
    def visitConstant_declaration(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#comment.
    def visitComment(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#identifier.
    def visitIdentifier(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#dtype.
    def visitDtype(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#ros_type.
    def visitRos_type(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#array_type.
    def visitArray_type(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#variable_array_type.
    def visitVariable_array_type(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#fixed_array_type.
    def visitFixed_array_type(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#integral_type.
    def visitIntegral_type(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#floating_point_type.
    def visitFloating_point_type(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#temportal_type.
    def visitTemportal_type(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#string_type.
    def visitString_type(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#boolean_type.
    def visitBoolean_type(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#sign.
    def visitSign(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#integral_value.
    def visitIntegral_value(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#floating_point_value.
    def visitFloating_point_value(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#bool_value.
    def visitBool_value(self, ctx):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by RosMessageParser#string_value.
    def visitString_value(self, ctx):
        return self.visitChildren(ctx)


