/** @odoo-module **/
const { xml, Component } = owl;
import { standardFieldProps } from "@web/views/fields/standard_field_props";
// Import the registry
import {registry} from "@web/core/registry";


export class DownloadButton extends Component {
    setup() {
        super.setup();
    }
}

DownloadButton.template = xml`<div><a role="button" class="btn btn-primary" t-att-href="props.value" download="">Download</a></div>`;
DownloadButton.props = standardFieldProps;

// Add the field to the correct category
registry.category("fields").add("download_button", DownloadButton);
