
    # def fetch_resource_list(self):
    #     self.ensure_one()
    #     if not self.data_source_type_id:
    #         raise MissingError("Source type is not set!")

    #     _logger.info(f"Fetching resources from data source {self.name}")
    #     r_list = self.data_source_type_id.fetch_resource_data()
    #     # first fetch
    #     if not self.last_fetch and not self.resource_ids:
    #         _logger.info(
    #             f"Creating resource objects for data source {self.name}"
    #         )
    #         self.resource_ids = [Command.create(r_data) for r_data in r_list]
    #         self.last_fetch = datetime.now()
    #         return True

    #     # It can be a very long loop, getting object beforehand when possible
    #     _logger.info("Syncing resource data...")
    #     model_resource = self.env['external.data.resource']
    #     self_id = self.id
    #     self_name = self.name
    #     data_source_type = self.data_source_type_id
    #     last_fetch = self.last_fetch
    #     new_resources = []
    #     i = 0
    #     limit = self.fetch_limit
    #     for r_data in r_list:
    #         i += 1
    #         # Don't process old data
    #         last_mod = r_data.get('last_mod')
    #         if not last_mod:
    #             continue

    #         r_name = r_data.get('name')
    #         if not r_name:
    #             r_name = data_source_type.get_resource_name(r_data)

    #         resource = model_resource.search([
    #             ('data_source_id', '=', self_id),
    #             ('name', '=', r_name),
    #             ('last_mod', '!=', last_mod),
    #         ], limit=1)
    #         # TODO: deduplication later...
    #         if resource:
    #             msg = "Updating resource object #{nr} of data source {ds}: {r}"
    #             _logger.info(msg.format(nr=i, ds=self_name, r=r_name))
    #             resource.write(r_data)
    #         else:
    #             new_resources.append(r_data)
    #         if i == limit:
    #             break

    #     self.resource_ids = [Command.create(r_data) for r_data in new_r_list]
    #     self.last_fetch = datetime.now()
    #     return True


# class ExternalDataPackage(models.Model):
#     _name = 'external.data.package'
#     _description = "External Data Package"

#     name = fields.Char(required=True)
#     url = fields.Char()
#     priority = fields.Float("Priority")
#     skip = fields.Boolean()
#     notes = fields.Text()
#     last_mod = fields.Datetime("Last modification")
#     last_pull = fields.Datetime("Last pull")
#     last_push = fields.Datetime("Last push")
#     valid_until = fields.Datetime("Valid until")
#     data_source_id = fields.Many2one(
#         'external.data.source',
#         ondelete='cascade',
#         string="Data source",
#         required=True,
#     )
#     field_mapping_ids = fields.Many2many(
#         comodel_name='external.data.field.mapping',
#         string="Type mappings",
#     )
#     object_ids = fields.Many2many(
#         comodel_name='external.data.object',
#         string="External objects",
#     )
#     # TODO: Language

#     def toggle_skip(self):
#         for record in self:
#             record.skip = not record.skip

#     def batch_pull(self, sync=False, prune=False, batch_size=1, do_all=False):
#         res = []
#         i = 0
#         for package in self:
#             try:
#                 res.append(package.pull(sync=sync, prune=prune))
#             except Exception as e:
#                 _logger.error(e)
#                 package.notes = ("Pull error:\n" + str(e))
#                 package.skip = True
#             i += 1
#             if i == batch_size and not do_all:
#                 break
#         return res

#     def pull(self, sync=False, prune=False):
#         self.ensure_one()
#         _logger.info(f"Pulling package {self.name}")
#         dataset = self.data_source_id.pull_package(self.id)

#         # find foreign_types
#         found_foreign_type_names = list(dataset.keys())
#         field_mappings = self.field_mapping_ids.search([
#             ('data_source_id', '=', self.data_source_id.id),
#             ('foreign_type_id.name', 'in', found_foreign_type_names),
#         ])
#         if field_mappings:
#             self.field_mapping_ids = [Command.set(field_mappings.ids)]

#         metadata = {
#             'package_id': self.id,
#             'direction': 'pull',
#         }
#         foreign_objects = []
#         for field_mapping in field_mappings:
#             foreign_type = field_mapping.foreign_type_id.name
#             foreign_id_key = field_mapping.foreign_id_field_id.name
#             index = 0
#             for data in dataset[foreign_type]:
#                 index += 1
#                 foreign_id = data.get(foreign_id_key)
#                 if not foreign_id:
#                     _logger.error(
#                         f"Missing foreign ID from package {self.name}"
#                     )
#                     continue
#                 foreign_objects.append((foreign_id, field_mapping.id))
#                 if not sync:
#                     continue

#                 # get external_object
#                 external_object = self.object_ids.search([
#                     ('field_mapping_id', '=', field_mapping.id),
#                     ('foreign_id', '=', foreign_id),
#                 ], limit=1)
#                 if not external_object:
#                     external_object = self.object_ids.create({
#                         'field_mapping_id': field_mapping.id,
#                         'foreign_id': foreign_id,
#                         'priority': index,
#                     })
#                     external_object.find_and_set_object_link_id()
#                 if self.id not in external_object.package_ids.ids:
#                     external_object.package_ids = [Command.link(self.id)]

#                 # pre processing
#                 metadata.update({
#                     'field_mapping_id': field_mapping.id,
#                     'foreign_type': foreign_type,
#                     'foreign_id': foreign_id,
#                     'record': external_object.object_link_id._record(),
#                     'pre_post': 'pre',
#                 })
#                 vals = field_mapping.apply_mapping(data, metadata)
#                 field_mapping.rule_ids_pre.apply_rules(vals, metadata)
#                 external_object.rule_ids_pre.apply_rules(vals, metadata)
#                 external_object.write_odoo_object(vals, metadata)

#                 # post processing
#                 metadata.update({
#                     'record': external_object.object_link_id._record(),
#                     'pre_post': 'post',
#                 })
#                 vals = field_mapping.apply_mapping(data, metadata)
#                 field_mapping.rule_ids_post.apply_rules(vals, metadata)
#                 external_object.rule_ids_post.apply_rules(vals, metadata)
#                 external_object.write_odoo_object(vals, metadata)

#                 self.last_pull = datetime.now()

#         if prune:
#             self.prune_objects(foreign_objects)

#         return dataset

#     def prune_objects(self, foreign_objects):
#         self.ensure_one()
#         found_object_ids = []
#         for foreign_id, field_mapping_id in self.object_ids:
#             found_object_ids += self.object_ids.filtered(lambda o: (
#                 o.foreign_id == foreign_id and
#                 o.field_mapping_id == field_mapping_id
#             )).ids
#         unrelated_object_ids = set(self.object_ids.ids) - set(found_object_ids)
#         self.object_ids = [Command.unlink(i) for i in unrelated_object_ids]
#         # TODO: delete orphan objects

#     def button_open(self):
#         self.ensure_one()
#         res_id = self.env.context.get('default_res_id')
#         return {
#             "type": "ir.actions.act_window",
#             "res_model": "external.data.package",
#             "views": [[False, "form"]],
#             "res_id": res_id,
#         }
